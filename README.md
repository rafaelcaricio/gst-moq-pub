# GStreamer MoQ Publisher Plugin

A GStreamer plugin that implements a Media over QUIC (MoQ) publisher element. This element allows publishing live media streams to MoQ relay servers using the WebTransport protocol.

## Overview

The `moqpublisher` element is a bin element that accepts video and/or audio input streams and publishes them to a MoQ relay server. It handles:

- WebTransport connection management
- CMAF packaging of media streams
- MoQ catalog generation and updates
- Track management and prioritization
- Init segment handling

## Installation

### Prerequisites

- Rust toolchain (latest stable version)
- GStreamer development files (>= 1.22)
- pkg-config

### Building

```bash
cargo build --release
```

## Element Properties

- `url` (string): The WebTransport URL of the MoQ relay server
- `namespace` (string): The namespace for the published tracks (default: "default")
- `fragment-duration` (uint64): Duration of each media fragment in milliseconds (default: 2000)
- `certificate-file` (string): Optional path to TLS certificate file for the WebTransport connection

## Pad Properties

Each sink pad supports the following properties via a "track-settings" structure:

- `track-name` (string): Name of the track in the MoQ catalog
- `priority` (uint8): Priority value for the track's fragments (0-255, default: 127)

## Internal Architecture

The element works by:

1. Creating a bin containing cmafmux and appsink elements for each input pad
2. Using cmafmux to package incoming media into CMAF fragments
3. Processing CMAF fragments into MoQ groups and objects
4. Managing WebTransport connections to the relay server
5. Generating and updating the MoQ catalog based on media capabilities
6. Publishing media tracks with proper prioritization

## Example Usage

The repository includes a video publishing example that can be run with:

```bash
cargo run --example videopub -- \
    --url https://localhost:4443 \
    --namespace example \
    --fragment-duration 2000 \
    --with-audio  # Optional: enable audio publishing
```

### Example Pipeline

```bash
gst-launch-1.0 \
    videotestsrc ! x264enc ! h264parse ! \
    moqpublisher url="https://localhost:4443" namespace="example" \
        fragment-duration=2000
```

### Building a Custom Pipeline

The element supports multiple input pads for different media tracks. Each pad must be requested with `request_pad_simple("sink_%u")` and configured with appropriate track settings:

```rust
let moqpub = gst::ElementFactory::make("moqpublisher")
    .property("url", "https://localhost:4443")
    .property("namespace", "example")
    .build()?;

// Request and configure video pad
let video_pad = moqpub.request_pad_simple("sink_%u").unwrap();
let video_settings = gst::Structure::builder("video1-rendition")
    .field("track-name", "video_1")
    .field("priority", 127u8)
    .build();
video_pad.set_property("track-settings", &video_settings);
```

## Debugging

The element supports GStreamer's debug system. Enable debug output with:

```bash
export GST_DEBUG=moqpublisher:4
```

The example program also generates pipeline graphs in dot format when running. These can be converted to images with:

```bash
dot -Tpng moq-publisher.dot > pipeline.png
```

## License

This project is licensed under the Mozilla Public License 2.0 - see the LICENSE file for details.
