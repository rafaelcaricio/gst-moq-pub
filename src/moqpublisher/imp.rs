// Copyright (C) 2025, Rafael Caricio <rafael@caricio.com>
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
// If a copy of the MPL was not distributed with this file, You can obtain one at
// <https://mozilla.org/MPL/2.0/>.
//
// SPDX-License-Identifier: MPL-2.0

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use tokio::sync::oneshot;
use url;
use futures::future::AbortHandle;
use gst::glib;
use gst::prelude::*;
use gst::subclass::prelude::*;
use gst_base::prelude::*;
use bytes::Bytes;
use futures::FutureExt;
use tokio::runtime;
use std::sync::LazyLock;
use gst::prelude::*;
use gst::subclass::prelude::*;
use gst_base::prelude::*;
use once_cell::sync::Lazy;
use moq_transport::serve::{self, ServeError};
use moq_transport::session::Publisher;
use anyhow::Error;
use std::sync::atomic::{AtomicU64, AtomicU32, Ordering};


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
    track_name: String,

    // Track priority (0-255)
    priority: u8,

    // Group identifier for correlated tracks
    group_id: u8,
}

impl From<gst::Structure> for MoqPublisherSinkPadSettings {
    fn from(s: gst::Structure) -> Self {
        MoqPublisherSinkPadSettings {
            track_name: s.get::<String>("track-name").unwrap(),
            priority: s.get::<u8>("priority").unwrap(),
            group_id: s.get::<u8>("group_id").unwrap(),
        }
    }
}

impl From<MoqPublisherSinkPadSettings> for gst::Structure {
    fn from(obj: MoqPublisherSinkPadSettings) -> Self {
        gst::Structure::builder("track-settings")
            .field("track-name", obj.track_name)
            .field("priority", obj.priority)
            .field("group_id", obj.group_id)
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
            "track-settings" => {
                Into::<gst::Structure>::into(settings.clone()).to_value()
            }
            _ => unimplemented!(),
        }
    }
}

impl GstObjectImpl for MoqPublisherSinkPad {}

impl PadImpl for MoqPublisherSinkPad {}

impl ProxyPadImpl for MoqPublisherSinkPad {}

impl GhostPadImpl for MoqPublisherSinkPad {}

impl MoqPublisherSinkPad {
    fn parent(&self) -> super::MoqPublisherSinkPad {
        self.obj()
            .parent()
            .map(|elem_obj| {
                elem_obj
                    .downcast::<super::MoqPublisherSinkPad>()
                    .expect("Wrong Element type")
            })
            .expect("Pad should have a parent at this stage")
    }
}

struct State {
    publisher: Option<Publisher>,
    tracks_writer: Option<serve::TracksWriter>,
    connection_task: Option<JoinHandle<()>>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            publisher: None,
            tracks_writer: None,
            connection_task: None,
        }
    }
}

#[derive(Debug, Clone)]
struct Settings {
    // MoQ relay URL to connect to using WebTransport
    url: String,

    // Path to client certificate file for WebTransport
    certificate_file: Option<PathBuf>,

    // Makes sure all cmafmux elements have the same duration
    fragment_duration: u64,

    // MoQ namespace for tracks
    namespace: String,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            url: String::new(),
            certificate_file: None,
            fragment_duration: 2000,
            namespace: String::from("default"),
        }
    }
}

struct TrackData {
    cmafmux: gst::Element,
    appsink: gst_app::AppSink,
    track_writer: Option<serve::GroupsWriter>,
    pad: String,
    sequence: AtomicU64,
    init_segment: Mutex<Option<gst::Buffer>>,

    // Media info for track, set after first caps
    media_info: Option<MediaInfo>,
}

#[derive(Clone)]
struct MediaInfo {
    mime_type: String,
    width: Option<u32>,
    height: Option<u32>,
    framerate: Option<gst::Fraction>,
    channels: Option<u32>,
    sample_rate: Option<u32>,
    language: Option<String>,
}

fn get_mime_type(caps: &gst::Caps) -> String {
    let mime = gst_pbutils::codec_utils_caps_get_mime_codec(caps);
    mime.map(|s| s.to_string()).unwrap_or_else(|_| "application/octet-stream".to_string())
}

impl MediaInfo {
    fn new(caps: &gst::Caps) -> Self {
        let mime_type = get_mime_type(caps);
        let structure = caps.structure(0).unwrap();
        let width = structure.get::<u32>("width").ok();
        let height = structure.get::<u32>("height").ok();
        let framerate = structure.get::<gst::Fraction>("framerate").ok();
        let channels = structure.get::<u32>("channels").ok();
        let sample_rate = structure.get::<u32>("rate").ok();
        let language = structure.get::<String>("language").ok();

        Self {
            mime_type,
            width,
            height,
            framerate,
            channels,
            sample_rate,
            language,
        }
    }
}

pub struct MoqPublisher {
    settings: Arc<Mutex<Settings>>,
    state: Arc<Mutex<State>>,

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
            state: Arc::new(Mutex::new(State::default())),
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
                    .default_value(2000)
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
    fn pad_templates() -> &'static [gst::PadTemplate] {
        static PAD_TEMPLATES: LazyLock<Vec<gst::PadTemplate>> = LazyLock::new(|| {
            // Caps are restricted by the cmafmux element negotiation inside our bin element
            let caps = gst::Caps::new_any();
            let sink_pad_template = gst::PadTemplate::with_gtype(
                "sink_%u",
                gst::PadDirection::Sink,
                gst::PadPresence::Request,
                &caps,
                super::MoqPublisher::static_type(),
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
            gst::error!(CAT, obj = element, "Cannot request pad in non-NULL/READY state");
            return None;
        }

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
            .property("fragment-duration", settings.fragment_duration.mseconds())
            .property("write-mehd", true)
            .property_from_str("header-update-mode", "update")
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

        // Add pad to element
        element.add_pad(&sink_pad).unwrap();
        element.child_added(sink_pad.upcast_ref::<gst::Object>(), &sink_pad.name());

        // Store track data
        let track_data = TrackData {
            cmafmux,
            appsink,
            track_writer: None, // Will be set up in READY->PAUSED
            pad: sink_pad.name().to_string(),
            sequence: AtomicU64::new(1), // Start at 1 since 0 is reserved for init segment
            init_segment: Mutex::new(None),
            media_info: None,
        };

        self.track_data.lock().unwrap().insert(
            sink_pad.name().to_string(),
            track_data
        );

        Some(sink_pad.upcast())
    }

    fn change_state(
        &self,
        transition: gst::StateChange,
    ) -> Result<gst::StateChangeSuccess, gst::StateChangeError> {
        match transition {
            gst::StateChange::ReadyToPaused => {
                // Setup WebTransport connection
                if let Err(e) = self.setup_connection() {
                    gst::error!(CAT, obj = self.obj(), "Failed to setup connection: {:?}", e);
                    return Err(e);
                }
                
                // Setup MoQ publisher
                if let Err(e) = self.setup_publisher() {
                    gst::error!(CAT, obj = self.obj(), "Failed to setup publisher: {:?}", e);
                    return Err(e);
                }
            }
            
            gst::StateChange::PausedToPlaying => {
                // Announce tracks
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

// ## Internal Architecture
// For each request sink pad, create:
// 1. A cmafmux element for CMAF packaging
// 2. An appsink element to receive packaged media
// 3. A MoQ track publisher instance using moq-transport
//
// ## CMAF Integration Requirements
//
// The cmafmux element outputs buffer lists with this structure:
//
// Media Header (optional, has DISCONT|HEADER flags)
//    - Contains ftyp/moov boxes
//    - Only present at start or during header updates
//    - Must be handled separately as initialization data
//
// Segment Header (has HEADER flag)
//    - First buffer in each media segment
//    - Contains timing information in buffer metadata
//
// Media Data Buffers
//    - Remaining buffers in list
//    - Contains actual media payload
//
// ## MoQ Transport Integration
//
// Use the moq-transport crate's API:
//
// Track Setup: ```rust // Create tracks container let (writer, _, reader) = serve::Tracks::new(namespace).produce();
// // Start announcing
// publisher.announce(reader).await?;
//
// // Create track
// let track = writer.create(&track_name)?;
// let track_writer = track.groups()?;
// ```
//
// Group Publishing: ```rust // Create group for segment let group = track_writer.create(serve::Group {     group_id: sequence_number,     priority: track_priority, })?;
// // Write data
// group.write(buffer_data)?;
// ```
//
// ## Key Implementation Requirements
//
// Handle initialization segments (ftyp/moov):
//    - Detect using DISCONT|HEADER flags
//    - Send as separate MoQ group
//    - Cache for header updates
//
// Handle media segments:
//    - Create new MoQ group for each CMAF segment
//    - Use sequence numbers from buffer timestamps
//    - Track priorities per sink pad
//
// Buffer Processing:
//    - Process all cmafmux buffer list components
//    - Extract timing info from segment headers
//    - Handle header updates correctly
//
// Error Handling:
//    - Handle MoQ connection failures
//    - Handle track announcement errors
//    - Clean up resources properly
impl MoqPublisher {
    fn setup_connection(&self) -> Result<(), gst::StateChangeError> {
        let settings = self.settings.lock().unwrap();
        let url = url::Url::parse(&settings.url)
            .map_err(|e| {
                gst::error!(CAT, obj = self.obj(), "Invalid URL {}: {}", settings.url, e);
                gst::StateChangeError
            })?;

        let state = self.state.clone();
        let cert_path = settings.certificate_file.clone();

        // Spawn connection task
        let handle = std::thread::spawn(move || {
            RUNTIME.block_on(async move {
                let mut tls = moq_native_ietf::tls::Args::default();
                if let Some(path) = cert_path {
                    tls.root = vec![path];
                }
                tls.disable_verify = true;

                let quic = moq_native_ietf::quic::Endpoint::new(moq_native_ietf::quic::Config {
                    bind: "[::]:0".parse().unwrap(),
                    tls: tls.load().unwrap(),
                }).unwrap();

                let session = quic.client.connect(&url).await.unwrap();
                let (session, publisher) = moq_transport::session::Publisher::connect(session).await.unwrap();

                let mut state = state.lock().unwrap();
                state.publisher = Some(publisher);

                session.run().await.unwrap();
            });
        });

        self.state.lock().unwrap().connection_task = Some(handle);

        Ok(())
    }

    fn setup_publisher(&self) -> Result<(), gst::StateChangeError> {
        let settings = self.settings.lock().unwrap();
        let namespace = settings.namespace.clone();
        drop(settings);

        // Wait for publisher to be available
        let mut retries = 0;
        while self.state.lock().unwrap().publisher.is_none() {
            if retries > 50 {
                gst::error!(CAT, obj = self.obj(), "Timeout waiting for publisher connection");
                return Err(gst::StateChangeError);
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
            retries += 1;
        }

        // Create tracks container
        let (writer, _, reader) = serve::Tracks::new(namespace).produce();

        let mut state = self.state.lock().unwrap();
        let publisher = state.publisher.as_mut().unwrap();

        // Start announcing tracks
        let announce_future = publisher.announce(reader);
        RUNTIME.block_on(announce_future).map_err(|e| {
            gst::error!(CAT, obj = self.obj(), "Failed to announce tracks: {}", e);
            gst::StateChangeError
        })?;

        state.tracks_writer = Some(writer);

        Ok(())
    }

    fn announce_tracks(&self) -> Result<(), gst::StateChangeError> {
        let mut state = self.state.lock().unwrap();
        let tracks_writer = state.tracks_writer.as_mut().ok_or_else(|| {
            gst::error!(CAT, obj = self.obj(), "No tracks writer available");
            gst::StateChangeError
        })?;

        let mut track_data = self.track_data.lock().unwrap();

        // Setup track writers for each pad
        for (_, track) in track_data.iter_mut() {
            let track_writer = tracks_writer.create(&track.pad).ok_or_else(|| {
                gst::error!(CAT, obj = self.obj(), "Failed to create track writer");
                gst::StateChangeError
            })?;

            track.track_writer = Some(track_writer.groups().unwrap());
        }

        Ok(())
    }

    fn cleanup_publisher(&self) {
        let mut state = self.state.lock().unwrap();
        
        // Clear track writers
        let mut track_data = self.track_data.lock().unwrap();
        for (_, track) in track_data.iter_mut() {
            track.track_writer = None;
        }

        // Clear publisher state
        state.publisher = None;
        state.tracks_writer = None;

        // Abort connection task if running
        if let Some(handle) = state.connection_task.take() {
            handle.join().ok();
        }
    }

    fn handle_cmaf_data(
        &self,
        track: &mut TrackData, 
        mut buffer_list: gst::BufferList
    ) -> Result<(), Error> {
        assert!(!buffer_list.is_empty());

        let mut first = buffer_list.get(0).unwrap();

        // Each list contains a full segment, i.e. does not start with a DELTA_UNIT
        assert!(!first.flags().contains(gst::BufferFlags::DELTA_UNIT));

        // Handle initialization segment
        if first.flags().contains(gst::BufferFlags::DISCONT | gst::BufferFlags::HEADER) {
            let mut init_guard = track.init_segment.lock().unwrap();
            *init_guard = Some(first.copy());
            
            // Create high priority group for init segment
            let mut group = track.track_writer.as_mut()
                .expect("track writer not initialized")
                .create(serve::Group {
                    group_id: 0,
                    priority: 0, // Highest priority
                })?;

            let map = first.map_readable().unwrap();
            group.write(Bytes::copy_from_slice(&map))?;
            drop(map);

            drop(init_guard);

            // Remove first buffer since it is our init segment
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

        let pad = self.obj().static_pad(&track.pad).unwrap().downcast::<super::MoqPublisherSinkPad>().unwrap();

        let segment_sequence = track.sequence.fetch_add(1, Ordering::SeqCst);
        let priority = pad.imp().settings.lock().unwrap().priority;

        // Create MoQ group for segment
        let mut group = track.track_writer.as_mut()
            .expect("track writer not initialized")
            .create(serve::Group {
                group_id: segment_sequence,
                priority,
            })?;

        // Write all buffers
        for buffer in buffer_list.iter() {
            let map = buffer.map_readable().unwrap(); 
            group.write(Bytes::copy_from_slice(&map))?;
        }

        Ok(())
    }
}
