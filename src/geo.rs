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
        let city: geoip2::City = self.reader.lookup(ip).ok()?;
        let location = city.location?;
        Some((location.latitude?, location.longitude?))
    }
}
