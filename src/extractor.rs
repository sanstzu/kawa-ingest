use bytes::Bytes;

pub fn extract_audio(data: Bytes) -> Bytes {
    let start_ptr = 2;
    let raw_bytes_size = data.len() - 2;
    let adts = {
        let mut adts: [u8; 7];

        adts = [0, 0, 0, 0, 0, 0, 0];

        let syncword: u16 = 0xFFF; // 12b
        let id: u8 = 0x00; // 1b
        let layer: u8 = 0x00; // 2b
        let protection_absent: u8 = 0x01; // 1b

        let profile: u8 = 0x01; // 2b
        let sampling_frequency_index: u8 = 0x03; // 4b, assuming it is 48kHz
        let private_bit: u8 = 0x00; // 1b
        let channel_configuration: u8 = 0x02; // 3b

        let original_copy: u8 = 0x00; // 1b
        let home: u8 = 0x00; // 1b
        let copyright_identification_bit: u8 = 0x00; // 1b
        let copyright_identification_start: u8 = 0x00; // 1b

        let aac_frame_length: u16 = (raw_bytes_size + 7) as u16; // 13b
        let adts_buffer_fullness: u16 = 0x7FF; // 11b
        let number_of_raw_data_blocks_in_frame: u8 = 0x00; // 2b

        adts[0] = (syncword >> 4) as u8;

        adts[1] = ((syncword & 0x0F) << 4) as u8;
        adts[1] |= (id << 3) as u8;
        adts[1] |= (layer << 1) as u8;
        adts[1] |= (protection_absent) as u8;

        adts[2] = (profile << 6) as u8;
        adts[2] |= (sampling_frequency_index << 2) as u8;
        adts[2] |= (private_bit << 1) as u8;
        adts[2] |= ((channel_configuration & 0b1) >> 2) as u8;

        adts[3] = ((channel_configuration & 0b11) << 6) as u8;
        adts[3] |= (original_copy << 5) as u8;
        adts[3] |= (home << 4) as u8;
        adts[3] |= (copyright_identification_bit << 3) as u8;
        adts[3] |= (copyright_identification_start << 2) as u8;
        adts[3] |= ((aac_frame_length >> 11) & 0b11) as u8;

        adts[4] = ((aac_frame_length >> 3) & 0xFF) as u8;

        adts[5] = ((aac_frame_length & 0b111) << 5) as u8;
        adts[5] |= ((adts_buffer_fullness >> 6) & 0b11111) as u8;

        adts[6] = ((adts_buffer_fullness & 0b111111) << 2) as u8;
        adts[6] |= (number_of_raw_data_blocks_in_frame & 0b11) as u8;

        adts
    };

    let mut res = Vec::new();
    // reads the size of the next NALU
    // writes the NALU to the file

    res.extend(&adts);
    res.extend(&data[start_ptr..]);

    Bytes::from(res)
}

// Extracts SPS and PPS NAL Units from the header details
pub fn extract_video_header(data: Bytes) -> Bytes {
    let nal_seperator_bytes = vec![0x00, 0x00, 0x00, 0x01];

    let mut res: Vec<u8> = Vec::new();

    let avc_decoder_configuration_record = data[5..].to_vec();

    let num_sps = avc_decoder_configuration_record[5] & 0x1F;

    let mut ptr = 6;

    for _ in 0..num_sps {
        let sps_size_b = avc_decoder_configuration_record[ptr..ptr + 2].to_vec();
        let sps_size = u16::from_be_bytes([sps_size_b[0], sps_size_b[1]]) as usize;
        ptr += 2;
        res.extend(&nal_seperator_bytes);
        res.extend(&avc_decoder_configuration_record[ptr..ptr + sps_size]);
        ptr += sps_size;
    }

    let num_pps = avc_decoder_configuration_record[ptr] & 0x1F;
    ptr += 1;

    for _ in 0..num_pps {
        let pps_size_b = avc_decoder_configuration_record[ptr..ptr + 2].to_vec();
        let pps_size = u16::from_be_bytes([pps_size_b[0], pps_size_b[1]]) as usize;
        ptr += 2;
        res.extend(&nal_seperator_bytes);
        res.extend(&avc_decoder_configuration_record[ptr..ptr + pps_size]);
        ptr += pps_size;
    }

    Bytes::from(res)
}

pub fn extract_video_frame(data: Bytes) -> Bytes {
    let nal_seperator_bytes: Vec<u8> = vec![0x00, 0x00, 0x00, 0x01];

    let mut res: Vec<u8> = Vec::new();

    let mut ptr = 5;
    let bytes_size = data.len();

    while ptr < bytes_size {
        // reads the size of the next NALU
        let size_b = data[ptr..ptr + 4].to_vec();
        let size = u32::from_le_bytes([size_b[3], size_b[2], size_b[1], size_b[0]]) as usize;
        // writes the NALU to the file
        res.extend(&nal_seperator_bytes);
        res.extend(&data[ptr + 4..ptr + 4 + size]);
        ptr = ptr + 4 + size;
    }
    Bytes::from(res)
}
