use clap::Parser;
use gst::glib;
use gst::prelude::*;
use std::sync::{Arc, Mutex};

/// Simple MoQ video publisher example
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// MoQ relay URL
    #[arg(short, long, default_value = "https://localhost:4443")]
    url: String,

    /// MoQ namespace
    #[arg(short, long, default_value = "example")]
    namespace: String,

    /// Fragment duration in milliseconds
    #[arg(short, long, default_value_t = 2000)]
    fragment_duration: u64,

    /// Optional client certificate file
    #[arg(short, long)]
    certificate_file: Option<String>,

    /// Enable audio publishing
    #[arg(long)]
    with_audio: bool,
}

fn setup_audio_elements(
    pipeline: &gst::Pipeline,
    moqpub: &gst::Element,
) -> Result<(), Box<dyn std::error::Error>> {
    let audiotestsrc = gst::ElementFactory::make("audiotestsrc")
        .property_from_str("wave", "ticks")
        .build()?;

    let audioenc = gst::ElementFactory::make("avenc_aac").build()?;

    let audio_capsfilter = gst::ElementFactory::make("capsfilter")
        .property(
            "caps",
            gst::Caps::builder("audio/mpeg")
                .field("mpegversion", 4i32)
                .build(),
        )
        .build()?;

    pipeline.add_many([&audiotestsrc, &audioenc, &audio_capsfilter])?;
    gst::Element::link_many(&[&audiotestsrc, &audioenc, &audio_capsfilter])?;

    let audio_pad = moqpub.request_pad_simple("sink_%u").unwrap();
    let audio_settings = gst::Structure::builder("audio1-rendition")
        .field("track-name", "audio_1")
        .field("priority", 100u8)
        .build();
    audio_pad.set_property("track-settings", &audio_settings);

    let audio_src_pad = audio_capsfilter.static_pad("src").unwrap();
    audio_src_pad.link(&audio_pad)?;

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    // Parse command line arguments
    let args = Args::parse();

    // Initialize GStreamer
    gst::init()?;
    gstmoq::plugin_register_static().unwrap();

    // Create pipeline
    let pipeline = gst::Pipeline::new();

    // Create moqpublisher
    let moqpub = gst::ElementFactory::make("moqpublisher")
        .property("url", args.url)
        .property("namespace", args.namespace)
        .property("fragment-duration", args.fragment_duration.mseconds())
        .build()?;

    // Add certificate file if specified
    if let Some(cert_file) = args.certificate_file {
        moqpub.set_property("certificate-file", cert_file);
    }

    // Create video source elements
    let videotestsrc = gst::ElementFactory::make("videotestsrc")
        .property_from_str("pattern", "ball")
        .build()?;

    let videoenc = gst::ElementFactory::make("x264enc")
        .property("bframes", 0u32)
        .property("key-int-max", i32::MAX as u32)
        .property_from_str("tune", "zerolatency")
        .build()?;

    let h264_capsfilter = gst::ElementFactory::make("capsfilter")
        .property(
            "caps",
            gst::Caps::builder("video/x-h264")
                .field("profile", "main")
                .build(),
        )
        .build()?;

    // Add elements to pipeline
    pipeline.add_many([&videotestsrc, &videoenc, &h264_capsfilter, &moqpub])?;

    // Link video elements
    gst::Element::link_many(&[&videotestsrc, &videoenc, &h264_capsfilter])?;

    // Setup moqpublisher pad
    let video_pad = moqpub.request_pad_simple("sink_%u").unwrap();
    let s = gst::Structure::builder("video1-rendition")
        .field("track-name", "video_1")
        .field("priority", 127u8)
        .build();
    video_pad.set_property("track-settings", &s);

    let caps_src_pad = h264_capsfilter.static_pad("src").unwrap();
    caps_src_pad.link(&video_pad)?;

    // After creating moqpublisher, add:
    if args.with_audio {
        setup_audio_elements(&pipeline, &moqpub)?;
    }

    // Create a main loop to run our GStreamer pipeline
    let main_loop = glib::MainLoop::new(None, false);
    let main_loop_clone = main_loop.clone();

    // Create an error flag that we can share between callbacks
    let error_occurred = Arc::new(Mutex::new(false));
    let error_occurred_clone = error_occurred.clone();

    // Add a bus watch to handle messages from the pipeline
    let bus = pipeline.bus().unwrap();
    let _watch = bus.add_watch({
        let pipeline_weak = pipeline.downgrade();
        move |_, msg| {
            let pipeline = match pipeline_weak.upgrade() {
                Some(pipeline) => pipeline,
                None => return glib::ControlFlow::Break,
            };
            use gst::MessageView;

            match msg.view() {
                MessageView::Eos(..) => {
                    println!("End of stream received");
                    main_loop_clone.quit();
                    return glib::ControlFlow::Break;
                }
                MessageView::Error(err) => {
                    let mut error_flag = error_occurred_clone.lock().unwrap();
                    *error_flag = true;
                    eprintln!(
                        "Error from {:?}: {} ({:?})",
                        err.src().map(|s| s.path_string()),
                        err.error(),
                        err.debug()
                    );
                    main_loop_clone.quit();
                    return glib::ControlFlow::Break;
                }
                MessageView::StateChanged(state) => {
                    // Only print state changes from the pipeline
                    if state
                        .src()
                        .map(|s| s == pipeline.upcast_ref::<gst::Object>())
                        .unwrap_or(false)
                    {
                        println!(
                            "Pipeline state changed from {:?} to {:?}",
                            state.old(),
                            state.current()
                        );
                    }
                }
                _ => (),
            }
            glib::ControlFlow::Continue
        }
    })?;

    // Setup signal handling for Ctrl+C
    ctrlc::set_handler({
        let main_loop = main_loop.clone();
        let pipeline = pipeline.downgrade();
        move || {
            println!("\nReceived interrupt signal, stopping pipeline...");
            let pipeline = match pipeline.upgrade() {
                Some(pipeline) => pipeline,
                None => {
                    main_loop.quit();
                    return;
                }
            };
            pipeline
                .debug_to_dot_file_with_ts(gst::DebugGraphDetails::all(), "moq-publisher-quitting");
            pipeline.send_event(gst::event::Eos::new());
            main_loop.quit();
        }
    })?;

    // Start playing
    pipeline.set_state(gst::State::Playing)?;
    println!("Pipeline is playing...");

    let weak_pipeline = pipeline.downgrade();
    glib::timeout_add_seconds_once(5, move || {
        let pipeline = match weak_pipeline.upgrade() {
            Some(pipeline) => pipeline,
            None => return,
        };

        pipeline.debug_to_dot_file_with_ts(gst::DebugGraphDetails::all(), "moq-publisher");
    });

    // Run the main loop
    main_loop.run();

    // Cleanup
    println!("Stopping pipeline...");
    pipeline.set_state(gst::State::Null)?;

    // Check if we're exiting due to an error
    if *error_occurred.lock().unwrap() {
        std::process::exit(1);
    }

    Ok(())
}
