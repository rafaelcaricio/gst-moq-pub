use gst::prelude::*;

fn init() {
    use std::sync::Once;
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        gst::init().unwrap();
    });
}

#[test]
fn test_element_basic_usage() {
    init();

    // Create test pipeline
    let pipeline = gst::Pipeline::new();

    // Create moqpublisher
    let moqpub = gst::ElementFactory::make("moqpublisher")
        .property("url", "https://localhost:4443")
        .property("namespace", "testmovie")
        .property("fragment-duration", 2000.mseconds())
        .build()
        .unwrap();

    // Create video source elements
    let videotestsrc = gst::ElementFactory::make("videotestsrc")
        .property("pattern", "ball")
        .build()
        .unwrap();
    let videoenc = gst::ElementFactory::make("x264enc")
        .property("bframes", 0u32)
        .property("key-int-max", i32::MAX as u32) // Let the muxer drive keyframe generation
        .property("tune", "zerolatency")
        .build()
        .unwrap();
    let h264_capsfilter = gst::ElementFactory::make("capsfilter")
        .property(
            "caps",
            gst::Caps::builder("video/x-h264")
                .field("profile", "main")
                .build(),
        )
        .build()
        .unwrap();

    // Add elements to pipeline
    pipeline
        .add_many([&videotestsrc, &videoenc, &h264_capsfilter, &moqpub])
        .unwrap();

    // Link video elements
    gst::Element::link_many(&[&videotestsrc, &videoenc, &h264_capsfilter]).unwrap();

    // setup moqpublisher pad
    let video_pad = moqpub.request_pad_simple("sink_0").unwrap();
    let s = gst::Structure::builder("video1-rendition")
        .field("track-name", "VIDEO")
        .field("priority", 127u8)
        .field("group-id", 1u8)
        .build();
    video_pad.set_property("track-settings", &s);

    let caps_src_pad = h264_capsfilter.static_pad("src").unwrap();
    caps_src_pad.link(&video_pad).unwrap();

    // Test state changes
    pipeline.set_state(gst::State::Paused).unwrap();

    // Wait for preroll
    let _ = pipeline.state(gst::ClockTime::from_seconds(5));

    // Cleanup
    pipeline.set_state(gst::State::Null).unwrap();
}
