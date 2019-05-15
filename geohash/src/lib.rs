use std::io::{Error, ErrorKind, Result};

static CODES_16: &'static [char; 16] =
    &['0', '1', '2', '3', '4', '5', '6', '7',
    '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'];
static MASKS_16: &'static [usize; 4] = &[1, 2, 4, 8];

pub fn encode_16(latitude: f32, longitude: f32, length: usize)
        -> Result<String> {
    let mut geohash = String::with_capacity(length);
    let (mut min_lat, mut max_lat, mut min_long, mut max_long) =
        (-90f32, 90f32, -180f32, 180f32);
    let mut index;

    if latitude < min_lat || latitude > max_lat {
        return Err(Error::new(ErrorKind::InvalidData, "invalid latitude"));
    } else if longitude < min_long || longitude > max_long {
        return Err(Error::new(ErrorKind::InvalidData, "invalid longitude"));
    }

    for _ in 0..length {
        index = 0;

        for i in 0..4 {
            if i % 2 == 0 { // latitude bit
                let mid = (min_lat + max_lat) / 2f32;
                if latitude > mid {
                    index |= MASKS_16[3 - i];
                    min_lat = mid;
                } else {
                    max_lat = mid;
                }
            } else { // longitude bit
                let mid = (min_long + max_long) / 2f32;
                if longitude > mid {
                    index |= MASKS_16[3-i];
                    min_long = mid;
                } else {
                    max_long = mid;
                }
            }
        }

        geohash.push(CODES_16[index]);
    }

    Ok(geohash)
}

pub fn decode_16(geohash: &str) -> Result<(f32, f32, f32, f32)> {
    let (mut min_lat, mut max_lat, mut min_long, mut max_long) =
        (-90f32, 90f32, -180f32, 180f32);
    let mut index;

    for c in geohash.chars() {
        index = match c as usize {
            x if x >= 48 && x <= 57 => x - 48,
            x if x >= 97 && x <= 102 => x - 87,
            _ => return Err(Error::new(
                ErrorKind::InvalidData, "invalid character")),
        };

        for i in 0..4 {
            let higher = (index & MASKS_16[3-i]) == MASKS_16[3-i];
            if i % 2 == 0 { // latitude bit
                let mid = (min_lat + max_lat) / 2f32;
                if higher {
                    min_lat = mid;
                } else {
                    max_lat = mid;
                }
            } else { // longitude bit
                let mid = (min_long + max_long) / 2f32;
                if higher {
                    index |= MASKS_16[3-i];
                    min_long = mid;
                } else {
                    max_long = mid;
                }
            }
        }
    }

    Ok((min_lat, max_lat, min_long, max_long))
}

#[cfg(test)]
mod tests {
    #[test]
    fn cycle_geohash() {
        // test computer science building at csu
        test_coordinates((40.573896, -105.083309), 4, 8);

        // test bridge in appleton, wi
        test_coordinates((44.259412, -88.389305), 4, 8);
    }

    fn test_coordinates(coord: (f32, f32),
            min_length: usize, max_length: usize) {
        for i in min_length..max_length {
            let encoded = super::encode_16(coord.0, coord.1, i)
                .expect("encoding geohash");
            let decoded = super::decode_16(&encoded)
                .expect("decoding geohash");

            assert!(decoded.0 <= coord.0 && coord.0 <= decoded.1);
            assert!(decoded.2 <= coord.1 && coord.1 <= decoded.3);
        }
    }
}
