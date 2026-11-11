use maxminddb::{geoip2, Reader};
use std::net::IpAddr;
use std::sync::Arc;

#[derive(Clone)]
pub struct GeoIp {
    reader: Arc<Reader<Vec<u8>>>,
}

impl GeoIp {
    pub fn open(path: &str) -> Result<Self, String> {
        let reader = Reader::open_readfile(path)
            .map_err(|e| format!("Failed to open GeoIP database '{path}': {e}"))?;
        Ok(Self {
            reader: Arc::new(reader),
        })
    }

    pub fn lookup(&self, ip_str: &str) -> Option<(f64, f64)> {
        let ip: IpAddr = ip_str.parse().ok()?;
        let city: geoip2::City = self.reader.lookup(ip).ok()?.decode().ok()??;
        Some((city.location.latitude?, city.location.longitude?))
    }
}

#[cfg(test)]
mod tests {
    use super::GeoIp;

    fn bundled() -> GeoIp {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/assets/GeoLite2-City.mmdb");
        GeoIp::open(path).expect("bundled GeoLite2 database opens")
    }

    #[test]
    fn public_address_resolves_to_coordinates() {
        let geo = bundled();
        for ip in ["8.8.8.8", "2001:4860:4860::8888", "81.2.69.142"] {
            let (lat, lon) = geo.lookup(ip).expect("public address has a location");
            assert!(
                (-90.0..=90.0).contains(&lat) && (-180.0..=180.0).contains(&lon),
                "{ip}: {lat},{lon}"
            );
        }
    }

    #[test]
    fn private_and_malformed_addresses_have_no_location() {
        let geo = bundled();
        for ip in ["127.0.0.1", "10.0.0.1", "not-an-ip", ""] {
            assert_eq!(geo.lookup(ip), None, "{ip}");
        }
    }
}
