// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use super::jsonize;
use image::{GenericImageView, Pixel};
use serde::{Deserialize, Serialize};

pub fn image_to_tensor(
    img: &[u8],
    model_width: u32,
    model_height: u32,
) -> anyhow::Result<(Vec<u8>, image::DynamicImage)> {
    let img = image::load_from_memory(img)?;
    if img.width() != model_width || img.height() != model_height {
        return Err(anyhow::anyhow!(
            "input image must be {}x{}, but actual {}x{}",
            model_width,
            model_height,
            img.width(),
            img.height()
        ));
    }

    // Convert from HWC (Height, Width, Channel) to CHW (Channel, Height, Width)
    // Normalize pixel values from [0, 255] to [0.0, 1.0]
    let mut tensor = vec![0u8; 3 * ((model_width * model_height) as usize) * 4];
    for y in 0..model_height {
        for x in 0..model_width {
            let pixel = img.get_pixel(x, y).to_rgb();
            let r_bytes = (pixel[0] as f32 / 255.0).to_ne_bytes();
            let g_bytes = (pixel[1] as f32 / 255.0).to_ne_bytes();
            let b_bytes = (pixel[2] as f32 / 255.0).to_ne_bytes();
            for j in 0..4 {
                tensor[((0 * model_width * model_height + y * model_width + x) as usize) * 4 + j] =
                    r_bytes[j];
                tensor[((1 * model_width * model_height + y * model_width + x) as usize) * 4 + j] =
                    g_bytes[j];
                tensor[((2 * model_width * model_height + y * model_width + x) as usize) * 4 + j] =
                    b_bytes[j];
            }
        }
    }
    Ok((tensor, img))
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Detection {
    pub bbox_x1: f32,
    pub bbox_y1: f32,
    pub bbox_x2: f32,
    pub bbox_y2: f32,
    pub class_label: String,
    pub class_id: usize,
    pub confidence: f32,
}
jsonize!(Detection);
