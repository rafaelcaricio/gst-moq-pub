
Issues to be fixed in the `moqpublisher` element code:
- Missing to keep the "broadcast" (with name broadcast to avoid confusion) in the element state
- Missing to create the init for every track with name "0.mp4" eg. `let init = broadcast.create("0.mp4")`
- Missing creating the ".catalog" track
- Missing to populate the catalog track with the tracks information as `moq_catalog::Root` object. Eg.:
    ```rust
    let catalog = moq_catalog::Root {
			version: 1,
			streaming_format: 1,
			streaming_format_version: "0.2".to_string(),
			streaming_delta_updates: true,
			common_track_fields: moq_catalog::CommonTrackFields::from_tracks(&mut tracks),
			tracks,
		};

		let catalog_str = serde_json::to_string_pretty(&catalog)?;
		log::info!("catalog: {}", catalog_str);

		// Create a single fragment for the segment.
		self.catalog.append(0)?.write(catalog_str.into())?;
    ```
- Missing to create a `moq_catalog::Track` for each track
- Missing to populate `let mut selection_params = moq_catalog::SelectionParam::default();` for each track


We have example code for all the above in the moq-pub project. We can use to learn how to apply that to our GStreamer element.