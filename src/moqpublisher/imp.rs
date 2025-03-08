// Copyright (C) 2025, Rafael Caricio <rafael@caricio.com>
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// <https://mozilla.org/MPL/2.0/>.
//
// SPDX-License-Identifier: MPL-2.0

use anyhow::Error;
use bytes::Bytes;
use gst::glib;
use gst::prelude::*;
use gst::subclass::prelude::*;
use moq_transport::coding::Tuple;
use moq_transport::serve;
use moq_transport::session::Publisher;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::LazyLock;
use std::sync::{Arc, Mutex};
use tokio::runtime;
use tokio::task::JoinHandle;
use url;

static CAT: Lazy<gst::DebugCategory> = Lazy::new(|| {
    gst::DebugCategory::new(
        "moqpublisher",
        gst::DebugColorFlags::empty(),
        Some("MoQ Publisher Element"),
    )
});

static RUNTIME: LazyLock<runtime::Runtime> = LazyLock::new(|| {
    runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .unwrap()
});

#[derive(Default, Clone)]
struct MoqPublisherSinkPadSettings {
    // MoQ track name for this pad
    track_name: Option<String>,

    // Track priority (0-255)
    priority: Option<u8>,
}

impl From<gst::Structure> for MoqPublisherSinkPadSettings {
    fn from(s: gst::Structure) -> Self {
        MoqPublisherSinkPadSettings {
            track_name: s.get_optional::<String>("track-name").unwrap(),
            priority: s.get_optional::<u8>("priority").unwrap(),
        }
    }
}

impl From<MoqPublisherSinkPadSettings> for gst::Structure {
    fn from(obj: MoqPublisherSinkPadSettings) -> Self {
        gst::Structure::builder("track-settings")
            .field_if_some("track-name", obj.track_name)
            .field_if_some("priority", obj.priority)
            .build()
    }
}

#[derive(Default)]
pub(crate) struct MoqPublisherSinkPad {
    settings: Mutex<MoqPublisherSinkPadSettings>,
}

#[glib::object_subclass]
impl ObjectSubclass for MoqPublisherSinkPad {
    const NAME: &'static str = "MoqPublisherSinkPad";
    type Type = super::MoqPublisherSinkPad;
    type ParentType = gst::GhostPad;
}

impl ObjectImpl for MoqPublisherSinkPad {
    fn properties() -> &'static [glib::ParamSpec] {
        static PROPERTIES: Lazy<Vec<glib::ParamSpec>> = Lazy::new(|| {
            vec![
                glib::ParamSpecBoxed::builder::<gst::Structure>("track-settings")
                    .nick("Track Settings")
                    .blurb("MoQ track settings")
                    .mutable_ready()
                    .build(),
            ]
        });

        PROPERTIES.as_ref()
    }

    fn set_property(&self, _id: usize, value: &glib::Value, pspec: &glib::ParamSpec) {
        match pspec.name() {
            "track-settings" => {
                let s = value
                    .get::<gst::Structure>()
                    .expect("Must be a valid structure");
                let mut settings = self.settings.lock().unwrap();
                *settings = s.into();
            }
            _ => unimplemented!(),
        }
    }

    fn property(&self, _id: usize, pspec: &glib::ParamSpec) -> glib::Value {
        let settings = self.settings.lock().unwrap();

        match pspec.name() {
            "track-settings" => Into::<gst::Structure>::into(settings.clone()).to_value(),
            _ => unimplemented!(),
        }
    }
}

impl GstObjectImpl for MoqPublisherSinkPad {}

impl PadImpl for MoqPublisherSinkPad {}

impl ProxyPadImpl for MoqPublisherSinkPad {}

impl GhostPadImpl for MoqPublisherSinkPad {}

struct State {
    broadcast: serve::TracksWriter,
    catalog: serve::SubgroupsWriter,
    connection_task: JoinHandle<()>,
    announce_task: JoinHandle<()>,
}

static DEFAULT_URL: &str = "";
static DEFAULT_CERTIFICATE_FILE: Option<PathBuf> = None;
static DEFAULT_FRAGMENT_DURATION: u64 = 2000;
static DEFAULT_CHUNK_DURATION: u64 = i64::MAX as u64;
static DEFAULT_NAMESPACE: &str = "default";

#[derive(Debug, Clone)]
struct Settings {
    // MoQ relay URL to connect to using WebTransport
    url: String,

    // Path to client certificate file for WebTransport
    certificate_file: Option<PathBuf>,

    // Makes sure all cmafmux GoP's duration are the same
    fragment_duration: u64,

    // Duration of each chunk
    chunk_duration: u64,

    // MoQ namespace for tracks
    namespace: String,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            url: DEFAULT_URL.to_string(),
            certificate_file: DEFAULT_CERTIFICATE_FILE.clone(),
            fragment_duration: DEFAULT_FRAGMENT_DURATION,
            chunk_duration: DEFAULT_CHUNK_DURATION,
            namespace: DEFAULT_NAMESPACE.to_string(),
        }
    }
}

struct TrackData {
    segment_track_name: Option<String>,
    segment_track: Option<serve::SubgroupsWriter>,
    init_track: Option<serve::SubgroupsWriter>,
    init_track_name: Option<String>,
    pad: String,
    sequence: AtomicU64,

    // Media info for track, set after the caps are fixed
    media_info: Option<MediaInfo>,
}

#[derive(Clone)]
struct MediaInfo {
    codec_mime: String,
    width: Option<i32>,
    height: Option<i32>,
    channels: Option<i32>,
    sample_rate: Option<i32>,
}

fn get_codec_mime_from_caps(caps: &gst::CapsRef) -> String {
    let mime = gst_pbutils::codec_utils_caps_get_mime_codec(caps);
    mime.map(|s| s.to_string())
        .unwrap_or_else(|_| "application/octet-stream".to_string())
}

impl MediaInfo {
    fn new(caps: &gst::CapsRef) -> Self {
        let codec_mime = get_codec_mime_from_caps(caps);
        let s = caps.structure(0).unwrap();
        let width = s.get::<i32>("width").ok();
        let height = s.get::<i32>("height").ok();
        let channels = s.get::<i32>("channels").ok();
        let sample_rate = s.get::<i32>("rate").ok();

        Self {
            codec_mime,
            width,
            height,
            channels,
            sample_rate,
        }
    }
}

pub struct MoqPublisher {
    settings: Arc<Mutex<Settings>>,
    state: Arc<Mutex<Option<State>>>,

    // Pad name -> TrackData
    track_data: Mutex<HashMap<String, TrackData>>,

    // Counter for generating unique sink pad names
    sink_pad_counter: AtomicU32,
}

#[glib::object_subclass]
impl ObjectSubclass for MoqPublisher {
    const NAME: &'static str = "GstMoqPublisher";
    type Type = super::MoqPublisher;
    type ParentType = gst::Bin;
    type Interfaces = (gst::ChildProxy,);

    fn new() -> Self {
        Self {
            settings: Arc::new(Mutex::new(Settings::default())),
            state: Arc::new(Mutex::new(None)),
            track_data: Mutex::new(HashMap::new()),
            sink_pad_counter: AtomicU32::new(0),
        }
    }
}

impl GstObjectImpl for MoqPublisher {}

impl BinImpl for MoqPublisher {}

impl ObjectImpl for MoqPublisher {
    fn properties() -> &'static [glib::ParamSpec] {
        static PROPERTIES: Lazy<Vec<glib::ParamSpec>> = Lazy::new(|| {
            vec![
                glib::ParamSpecString::builder("url")
                    .nick("URL")
                    .blurb("MoQ relay URL")
                    .build(),
                glib::ParamSpecString::builder("certificate-file")
                    .nick("Certificate File")
                    .blurb("Path to client certificate file")
                    .build(),
                glib::ParamSpecUInt64::builder("fragment-duration")
                    .nick("Fragment Duration")
                    .blurb("CMAF fragment duration in milliseconds")
                    .default_value(DEFAULT_FRAGMENT_DURATION)
                    .build(),
                glib::ParamSpecUInt64::builder("chunk-duration")
                    .nick("Chunk Duration")
                    .blurb("Duration of each chunk in milliseconds")
                    .default_value(DEFAULT_CHUNK_DURATION)
                    .build(),
                glib::ParamSpecString::builder("namespace")
                    .nick("Namespace")
                    .blurb("MoQ namespace for tracks")
                    .default_value("default")
                    .build(),
            ]
        });
        PROPERTIES.as_ref()
    }

    fn set_property(&self, _id: usize, value: &glib::Value, pspec: &glib::ParamSpec) {
        let mut settings = self.settings.lock().unwrap();
        match pspec.name() {
            "url" => {
                settings.url = value.get::<String>().unwrap();
            }
            "certificate-file" => {
                if let Ok(path) = value.get::<String>() {
                    settings.certificate_file = Some(PathBuf::from(path));
                } else {
                    settings.certificate_file = None;
                }
            }
            "fragment-duration" => {
                settings.fragment_duration = value.get::<u64>().unwrap();
            }
            "chunk-duration" => {
                settings.chunk_duration = value.get::<u64>().unwrap();
            }
            "namespace" => {
                settings.namespace = value.get::<String>().unwrap();
            }
            _ => unimplemented!(),
        }
    }

    fn property(&self, _id: usize, pspec: &glib::ParamSpec) -> glib::Value {
        let settings = self.settings.lock().unwrap();
        match pspec.name() {
            "url" => settings.url.to_value(),
            "certificate-file" => settings
                .certificate_file
                .as_ref()
                .map(|p| p.to_str().unwrap().to_string())
                .to_value(),
            "fragment-duration" => settings.fragment_duration.to_value(),
            "chunk-duration" => settings.chunk_duration.to_value(),
            "namespace" => settings.namespace.to_value(),
            _ => unimplemented!(),
        }
    }
}

impl ChildProxyImpl for MoqPublisher {
    fn children_count(&self) -> u32 {
        let object = self.obj();
        object.num_pads() as u32
    }

    fn child_by_name(&self, name: &str) -> Option<glib::Object> {
        let object = self.obj();
        object
            .pads()
            .into_iter()
            .find(|p| p.name() == name)
            .map(|p| p.upcast())
    }

    fn child_by_index(&self, index: u32) -> Option<glib::Object> {
        let object = self.obj();
        object
            .pads()
            .into_iter()
            .nth(index as usize)
            .map(|p| p.upcast())
    }
}

impl ElementImpl for MoqPublisher {
    fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
        static ELEMENT_METADATA: LazyLock<gst::subclass::ElementMetadata> = LazyLock::new(|| {
            gst::subclass::ElementMetadata::new(
                "MoQ Publisher Sink",
                "Source/Network/QUIC/MoQ",
                "Send media data to a MoQ relay server",
                "Rafael Caricio <rafael@caricio.com>",
            )
        });
        Some(&*ELEMENT_METADATA)
    }

    fn pad_templates() -> &'static [gst::PadTemplate] {
        static PAD_TEMPLATES: LazyLock<Vec<gst::PadTemplate>> = LazyLock::new(|| {
            // Caps are restricted by the cmafmux element negotiation inside our bin element
            let sink_pad_template = gst::PadTemplate::with_gtype(
                "sink_%u",
                gst::PadDirection::Sink,
                gst::PadPresence::Request,
                &gst::Caps::new_any(),
                super::MoqPublisherSinkPad::static_type(),
            )
            .unwrap();

            vec![sink_pad_template]
        });

        PAD_TEMPLATES.as_ref()
    }

    fn request_new_pad(
        &self,
        templ: &gst::PadTemplate,
        _name: Option<&str>,
        _caps: Option<&gst::Caps>,
    ) -> Option<gst::Pad> {
        let element = self.obj();

        // Only allow pad requests in NULL/READY state
        if element.current_state() > gst::State::Ready {
            gst::error!(
                CAT,
                obj = element,
                "Cannot request pad in non-NULL/READY state"
            );
            return None;
        }

        gst::info!(CAT, obj = element, "Requesting new sink pad");

        let settings = self.settings.lock().unwrap();

        // Generate unique pad name
        let pad_num = self.sink_pad_counter.fetch_add(1, Ordering::SeqCst);
        let pad_name = format!("sink_{}", pad_num);

        // Create the sink pad
        let sink_pad = gst::PadBuilder::<super::MoqPublisherSinkPad>::from_template(templ)
            .name(&pad_name)
            .build();

        // Create internal elements
        let cmafmux = gst::ElementFactory::make("cmafmux")
            .property("fragment-duration", settings.fragment_duration)
            .property("chunk-duration", settings.chunk_duration)
            .property_from_str("header-update-mode", "update")
            .property("write-mehd", true)
            .build()
            .unwrap();

        let appsink = gst_app::AppSink::builder()
            .name(pad_name.clone())
            .buffer_list(true)
            .sync(true)
            .callbacks({
                let this = self.downgrade();

                gst_app::AppSinkCallbacks::builder()
                    .new_sample(move |appsink| {
                        let this = match this.upgrade() {
                            Some(this) => this,
                            None => return Ok(gst::FlowSuccess::Ok),
                        };

                        let sample = appsink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                        let buffer_list = sample.buffer_list_owned().expect("no buffer list");

                        let mut track_data = this.track_data.lock().unwrap();
                        let track = track_data.get_mut(&pad_name).unwrap();

                        gst::debug!(
                            CAT,
                            obj = this.obj(),
                            "Received buffer list with {} buffers",
                            buffer_list.len()
                        );

                        if let Err(err) = this.handle_cmaf_data(track, buffer_list) {
                            gst::error!(CAT, "Failed to handle CMAF data: {}", err);
                            return Err(gst::FlowError::Error);
                        }

                        Ok(gst::FlowSuccess::Ok)
                    })
                    .build()
            })
            .build();

        // Add elements to bin
        element.add_many([&cmafmux, appsink.upcast_ref()]).unwrap();
        gst::Element::link_many([&cmafmux, appsink.upcast_ref()]).unwrap();

        // Create ghost pad
        let muxer_sink = cmafmux.static_pad("sink").unwrap();
        sink_pad.set_target(Some(&muxer_sink)).unwrap();

        // Add probe for caps events
        sink_pad.add_probe(gst::PadProbeType::EVENT_BOTH, {
            let this = self.downgrade();
            move |pad, info| {
                let this = match this.upgrade() {
                    Some(this) => this,
                    None => return gst::PadProbeReturn::Remove,
                };

                if let Some(gst::PadProbeData::Event(ref event)) = info.data {
                    if let gst::EventView::Caps(caps_evt) = event.view() {
                        let caps = caps_evt.caps();
                        gst::debug!(
                            CAT,
                            obj = this.obj(),
                            "Received caps event on pad {}: {:?}",
                            pad.name(),
                            caps
                        );

                        // Create MediaInfo from the caps
                        let media_info = MediaInfo::new(caps);

                        // Store the media info in track data
                        let mut track_data = this.track_data.lock().unwrap();
                        gst::debug!(
                            CAT,
                            obj = this.obj(),
                            "Updating media info for track {}",
                            pad.name()
                        );
                        if let Some(track) = track_data.get_mut(&pad.name().to_string()) {
                            track.media_info = Some(media_info);
                            gst::debug!(
                                CAT,
                                obj = this.obj(),
                                "Updated media info for track {}",
                                pad.name()
                            );

                            // Check if all tracks have media info and create catalog if ready
                            if track_data.values().all(|t| t.media_info.is_some()) {
                                gst::debug!(
                                    CAT,
                                    obj = this.obj(),
                                    "All tracks have media info, creating catalog"
                                );
                                drop(track_data);
                                if let Err(e) = this.create_catalog() {
                                    gst::error!(CAT, obj = pad, "Failed to create catalog: {}", e);
                                }
                            } else {
                                gst::debug!(
                                    CAT,
                                    obj = this.obj(),
                                    "Not all tracks have media info yet"
                                );
                            }
                        }
                    }
                }

                gst::PadProbeReturn::Ok
            }
        });

        // Add pad to element
        element.add_pad(&sink_pad).unwrap();
        element.child_added(sink_pad.upcast_ref::<gst::Object>(), &sink_pad.name());

        // Store track data
        let track_data = TrackData {
            segment_track: None, // Will be set up in READY->PAUSED
            segment_track_name: None,
            init_track: None, // Will be set up in READY->PAUSED
            init_track_name: None,
            pad: sink_pad.name().to_string(),
            sequence: AtomicU64::new(1), // Start at 1 since 0 is reserved for init segment
            media_info: None,
        };

        self.track_data
            .lock()
            .unwrap()
            .insert(sink_pad.name().to_string(), track_data);

        Some(sink_pad.upcast())
    }

    fn change_state(
        &self,
        transition: gst::StateChange,
    ) -> Result<gst::StateChangeSuccess, gst::StateChangeError> {
        match transition {
            gst::StateChange::ReadyToPaused => {
                // Setup connection and publisher in one go
                if let Err(e) = self.setup_connection() {
                    gst::error!(CAT, obj = self.obj(), "Failed to setup connection: {:?}", e);
                    return Err(e);
                }

                // Announce tracks after connection setup
                if let Err(e) = self.announce_tracks() {
                    gst::error!(CAT, obj = self.obj(), "Failed to announce tracks: {:?}", e);
                    return Err(e);
                }
            }

            gst::StateChange::PausedToReady => {
                // Cleanup publisher
                self.cleanup_publisher();
            }

            _ => (),
        }

        // Chain up
        self.parent_change_state(transition)
    }
}

impl MoqPublisher {
    fn setup_connection(&self) -> Result<(), gst::StateChangeError> {
        let settings = self.settings.lock().unwrap();
        let url = url::Url::parse(&settings.url).map_err(|e| {
            gst::error!(CAT, obj = self.obj(), "Invalid URL {}: {}", settings.url, e);
            gst::StateChangeError
        })?;
        let namespace = Tuple::from_utf8_path(&settings.namespace.clone());
        let cert_path = settings.certificate_file.clone();
        drop(settings);

        // Setup connection synchronously in the runtime
        let (broadcast, catalog, connection_task, announce_task) = RUNTIME.block_on(async {
            let mut tls = moq_native_ietf::tls::Args::default();
            if let Some(path) = cert_path {
                tls.root = vec![path];
            }
            tls.disable_verify = true;

            let quic = moq_native_ietf::quic::Endpoint::new(moq_native_ietf::quic::Config {
                bind: "[::]:0".parse().unwrap(),
                tls: tls.load().map_err(|e| {
                    gst::error!(CAT, obj = self.obj(), "Failed to load TLS config: {}", e);
                    gst::StateChangeError
                })?,
            })
            .map_err(|e| {
                gst::error!(
                    CAT,
                    obj = self.obj(),
                    "Failed to create QUIC endpoint: {}",
                    e
                );
                gst::StateChangeError
            })?;

            let session = quic.client.connect(&url).await.map_err(|e| {
                gst::error!(CAT, obj = self.obj(), "Failed to connect: {}", e);
                gst::StateChangeError
            })?;

            let (session, mut publisher) = Publisher::connect(session).await.map_err(|e| {
                gst::error!(CAT, obj = self.obj(), "Failed to create publisher: {}", e);
                gst::StateChangeError
            })?;

            // Create tracks container
            let (mut broadcast, _, reader) = serve::Tracks::new(namespace).produce();

            // Create catalog track
            let catalog = broadcast
                .create(".catalog")
                .ok_or_else(|| gst::StateChangeError)?
                .groups()
                .map_err(|_| gst::StateChangeError)?;

            // Create connection task
            let connection_task = tokio::spawn({
                let el_weak = self.obj().downgrade();
                async move {
                    let element = el_weak.upgrade().unwrap();
                    gst::debug!(CAT, obj = element, "Starting session");
                    if let Err(e) = session.run().await {
                        gst::error!(CAT, obj = element, "Session error: {}", e);
                        element.send_event(gst::event::Eos::new());
                    }
                    gst::debug!(CAT, obj = element, "Session done!");
                }
            });

            // Create announce task
            let announce_task = tokio::spawn({
                let el_weak = self.obj().downgrade();
                async move {
                    let element = el_weak.upgrade().unwrap();
                    gst::debug!(CAT, obj = element, "Announcing tracks");
                    if let Err(e) = publisher.announce(reader).await {
                        gst::error!(CAT, obj = element, "Publisher announce error: {}", e);
                        element.send_event(gst::event::Eos::new());
                    }
                    gst::debug!(CAT, obj = element, "Announce done!");
                }
            });

            Ok::<_, gst::StateChangeError>((broadcast, catalog, connection_task, announce_task))
        })?;

        gst::info!(CAT, obj = self.obj(), "Connected to MoQ relay at {}", url);

        // Store new state
        *self.state.lock().unwrap() = Some(State {
            broadcast,
            catalog,
            connection_task,
            announce_task,
        });

        Ok(())
    }

    fn announce_tracks(&self) -> Result<(), gst::StateChangeError> {
        let mut state_guard = self.state.lock().unwrap();
        let state = state_guard.as_mut().ok_or_else(|| {
            gst::error!(CAT, obj = self.obj(), "Not in ready state");
            gst::StateChangeError
        })?;

        let mut track_data = self.track_data.lock().unwrap();

        for (pad_name, track) in track_data.iter_mut() {
            let pad = self
                .obj()
                .static_pad(pad_name)
                .unwrap()
                .downcast::<super::MoqPublisherSinkPad>()
                .unwrap();

            let track_name = pad
                .imp()
                .settings
                .lock()
                .unwrap()
                .track_name
                .clone()
                .unwrap_or_else(|| pad_name.clone());
            let init_track_name = format!("{}_init.mp4", track_name);
            let segment_track_name = format!("{}.m4s", track_name);

            // Create per-track init segment track
            let init_track = state
                .broadcast
                .create(&init_track_name)
                .ok_or_else(|| {
                    gst::error!(CAT, obj = self.obj(), "Failed to create init track");
                    gst::StateChangeError
                })?
                .groups()
                .map_err(|_| gst::StateChangeError)?;

            track.init_track = Some(init_track);

            // Create media track
            let track_writer = state.broadcast.create(&segment_track_name).ok_or_else(|| {
                gst::error!(CAT, obj = self.obj(), "Failed to create track writer");
                gst::StateChangeError
            })?;

            track.init_track_name = Some(init_track_name);
            track.segment_track_name = Some(segment_track_name);

            track.segment_track = Some(track_writer.groups().map_err(|_| {
                gst::error!(CAT, obj = self.obj(), "Failed to create segment track");
                gst::StateChangeError
            })?);
        }

        Ok(())
    }

    fn create_catalog(&self) -> Result<(), Error> {
        let mut state_guard = self.state.lock().unwrap();
        let state = state_guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Not in ready state"))?;

        let mut tracks = Vec::new();
        let mut track_data = self.track_data.lock().unwrap();

        for (pad_name, track) in track_data.iter_mut() {
            let media_info = track.media_info.as_ref().ok_or_else(|| {
                anyhow::anyhow!("Missing media info for track on pad {}", pad_name)
            })?;

            let mut selection_params = moq_catalog::SelectionParam::default();

            // Use get_mime_type for codec string
            selection_params.codec = Some(media_info.codec_mime.clone());

            // Set video-specific parameters
            if let (Some(width), Some(height)) = (media_info.width, media_info.height) {
                selection_params.width = Some(width as u32);
                selection_params.height = Some(height as u32);
            }

            // Set audio-specific parameters
            if let Some(channels) = media_info.channels {
                selection_params.channel_config = Some(channels.to_string());
            }
            if let Some(sample_rate) = media_info.sample_rate {
                selection_params.samplerate = Some(sample_rate as u32);
            }

            let catalog_track = moq_catalog::Track {
                init_track: track.init_track_name.clone(),
                name: track.segment_track_name.clone().unwrap(),
                namespace: Some(state.broadcast.namespace.to_utf8_path()),
                packaging: Some(moq_catalog::TrackPackaging::Cmaf),
                render_group: Some(1),
                selection_params,
                ..Default::default()
            };

            tracks.push(catalog_track);
        }

        let catalog = moq_catalog::Root {
            version: 1,
            streaming_format: 1,
            streaming_format_version: "0.2".to_string(),
            streaming_delta_updates: true,
            common_track_fields: moq_catalog::CommonTrackFields::from_tracks(&mut tracks),
            tracks,
        };

        let catalog_str = serde_json::to_string_pretty(&catalog)?;
        gst::info!(CAT, obj = self.obj(), "MoQ catalog: {}", catalog_str);

        // Write catalog to track
        state
            .catalog
            .append(0)
            .map_err(|e| {
                gst::error!(CAT, obj = self.obj(), "Failed to append catalog: {}", e);
                gst::StateChangeError
            })?
            .write(catalog_str.into())
            .map_err(|e| {
                gst::error!(CAT, obj = self.obj(), "Failed to write catalog: {}", e);
                gst::StateChangeError
            })?;

        gst::debug!(CAT, obj = self.obj(), "Published catalog");

        Ok(())
    }

    fn cleanup_publisher(&self) {
        let mut state = self.state.lock().unwrap();

        // Clear track writers
        let mut track_data = self.track_data.lock().unwrap();
        for (_, track) in track_data.iter_mut() {
            track.segment_track = None;
        }

        // Take and clean up state if it exists
        if let Some(state) = state.take() {
            // Abort both tasks
            state.connection_task.abort();
            state.announce_task.abort();

            // Wait for both tasks
            let _ = RUNTIME.block_on(async {
                let _ = state.connection_task.await;
                let _ = state.announce_task.await;
            });
        }
    }

    fn handle_cmaf_data(
        &self,
        track: &mut TrackData,
        mut buffer_list: gst::BufferList,
    ) -> Result<(), Error> {
        assert!(!buffer_list.is_empty());

        let mut first = buffer_list.get(0).unwrap();

        // Handle initialization segment
        if first
            .flags()
            .contains(gst::BufferFlags::DISCONT | gst::BufferFlags::HEADER)
        {
            // Write to track's init track
            let init_track = track.init_track.as_mut().ok_or_else(|| {
                anyhow::anyhow!("Init track not initialized for pad {}", track.pad)
            })?;

            let mut group = init_track.append(0)?;
            let map = first.map_readable().unwrap();
            group.write(Bytes::copy_from_slice(&map))?;
            drop(map);

            gst::debug!(
                CAT,
                obj = self.obj(),
                "Wrote init segment for track {}",
                track.pad
            );

            // Remove init segment from buffer list and continue with media data
            buffer_list.make_mut().remove(0..1);

            if buffer_list.is_empty() {
                return Ok(());
            }
            first = buffer_list.get(0).unwrap();
        }

        // Process the rest of the media segment
        if !first.flags().contains(gst::BufferFlags::HEADER) {
            return Err(anyhow::anyhow!("Missing segment header"));
        }

        let pad = self
            .obj()
            .static_pad(&track.pad)
            .unwrap()
            .downcast::<super::MoqPublisherSinkPad>()
            .unwrap();

        let segment_sequence = track.sequence.fetch_add(1, Ordering::SeqCst);
        let priority = pad.imp().settings.lock().unwrap().priority;

        // Create MoQ group for segment
        let mut group = track
            .segment_track
            .as_mut()
            .expect("track writer not initialized")
            .create(serve::Subgroup {
                group_id: segment_sequence,
                priority: priority.unwrap_or(127),
                subgroup_id: 0,
            })?;

        // Write all buffers
        for buffer in buffer_list.iter() {
            let map = buffer.map_readable().unwrap();
            group.write(Bytes::copy_from_slice(&map))?;
        }

        gst::debug!(
            CAT,
            obj = self.obj(),
            "Published group {} for pad {}",
            segment_sequence,
            track.pad
        );

        Ok(())
    }
}
