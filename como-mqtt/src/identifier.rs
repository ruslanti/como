use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::mem;

use tracing::trace;

type UsedArray = Box<[usize; PacketIdentifier::SIZE]>;

pub struct PacketIdentifier {
    identifier: u16,
    used: UsedArray,
}

impl PacketIdentifier {
    const SIZE: usize = (u16::MAX as usize + 1) / (mem::size_of::<usize>() * 8);

    pub fn value(&self) -> u16 {
        self.identifier
    }

    pub fn release(&mut self, id: u16) {
        let id = id as usize;
        let value = &mut self.used[id / (mem::size_of::<usize>() * 8)];
        let mask: usize = 1 << (id % (mem::size_of::<usize>() * 8));
        *value &= !mask;
        trace!("drop PacketIdentifier {} ", id);
    }
}

impl Default for PacketIdentifier {
    fn default() -> Self {
        Self {
            identifier: 0,
            used: Box::new([0; Self::SIZE]),
        }
    }
}

impl Borrow<u16> for PacketIdentifier {
    fn borrow(&self) -> &u16 {
        &self.identifier
    }
}

impl Ord for PacketIdentifier {
    fn cmp(&self, other: &Self) -> Ordering {
        self.identifier.cmp(&other.identifier)
    }
}

impl PartialOrd for PacketIdentifier {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for PacketIdentifier {}

impl PartialEq for PacketIdentifier {
    fn eq(&self, other: &Self) -> bool {
        self.identifier == other.identifier
    }
}

impl Iterator for PacketIdentifier {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        let packet_identifier = self.identifier;
        let (packet_identifier, _) = packet_identifier.overflowing_add(1);

        let id = packet_identifier as usize;
        let value = &mut self.used[id / (mem::size_of::<usize>() * 8)];
        let mask: usize = 1 << (id % (mem::size_of::<usize>() * 8));

        if (*value & mask) != 0 {
            None
        } else {
            *value |= mask;

            self.identifier = packet_identifier;
            Some(packet_identifier)
        }
    }
}

impl fmt::Debug for PacketIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.identifier)
    }
}

impl fmt::Display for PacketIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.identifier)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packet_identifier() {
        let mut sequence: PacketIdentifier = Default::default();
        for i in 1..u16::MAX {
            let i1 = sequence.next().unwrap();
            assert_eq!(i, i1, "{} == {}", i, i1);
            sequence.release(i1);
        }
        assert_eq!(65535, sequence.next().unwrap());
        assert_eq!(0, sequence.next().unwrap());
        assert_eq!(1, sequence.next().unwrap());
        assert_eq!(2, sequence.next().unwrap());
        sequence.release(65535);
        sequence.release(0);
        sequence.release(1);
        sequence.release(2);
    }
}
