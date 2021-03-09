use std::cell::RefCell;
use std::mem;
use std::rc::Rc;

use anyhow::anyhow;
use anyhow::Result;
use tracing::debug;

type UsedArray = Rc<RefCell<Box<[usize; Sequence::SIZE]>>>;

pub struct PacketIdentifier {
    identifier: u16,
    used: UsedArray,
}

pub struct Sequence {
    previous: u16,
    used: UsedArray,
}
/*
impl From<PacketIdentifier> for u16 {
    fn from(p: PacketIdentifier) -> Self {
        p.identifier
    }
}*/

impl PacketIdentifier {
    pub(crate) fn value(&self) -> u16 {
        self.identifier
    }
}

impl Drop for PacketIdentifier {
    fn drop(&mut self) {
        let id = self.identifier as usize;
        let value = &mut self.used.borrow_mut()[id / (mem::size_of::<usize>() * 8)];
        let mask: usize = 1 << (id % (mem::size_of::<usize>() * 8));
        *value &= !mask;
        debug!("drop PacketIdentifier {} ", id);
    }
}

impl Sequence {
    const SIZE: usize = (u16::MAX as usize + 1) / (mem::size_of::<usize>() * 8);

    pub(crate) fn next(&mut self) -> Result<PacketIdentifier> {
        let packet_identifier = self.previous;
        let (packet_identifier, _) = packet_identifier.overflowing_add(1);

        let id = packet_identifier as usize;
        let value = &mut self.used.borrow_mut()[id / (mem::size_of::<usize>() * 8)];
        let mask: usize = 1 << (id % (mem::size_of::<usize>() * 8));

        if (*value & mask) != 0 {
            return Err(anyhow!("packet identifier overflow"));
        }

        *value |= mask;

        self.previous = packet_identifier;
        Ok(PacketIdentifier {
            identifier: packet_identifier,
            used: self.used.clone(),
        })
    }
}

impl Default for Sequence {
    fn default() -> Self {
        Sequence {
            used: Rc::new(RefCell::new(Box::new([0; Self::SIZE]))),
            previous: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence() {
        let mut sequence: Sequence = Default::default();
        for i in 1..u16::MAX {
            let id = sequence.next().unwrap();
            let i1 = id.value();
            assert_eq!(i, i1, "{} == {}", i, i1);
        }
        assert_eq!(65535, sequence.next().unwrap().value());
        assert_eq!(0, sequence.next().unwrap().value());
        assert_eq!(1, sequence.next().unwrap().value());
        assert_eq!(2, sequence.next().unwrap().value());
    }
}
