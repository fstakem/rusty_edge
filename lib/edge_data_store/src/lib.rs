pub mod in_memory;

extern crate edge_core;

use edge_core::Event;

pub use self::in_memory::InMemory;


// Data types
// -------------------------------------------------------------------------------------------------
trait Store {
    fn add_events(&mut self, events: Vec<Event>) -> ();
    fn get_window(&self, win_len_ms: i64) -> Vec<Event>;
    fn get_window_of_n(&self, n: u64) -> Vec<Event>;
}


// Tests
// -------------------------------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
