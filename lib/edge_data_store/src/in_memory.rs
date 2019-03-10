use std::collections::VecDeque;
use chrono::prelude::*;

use edge_core::Event;
use super::Store;

pub struct InMemory {
    buffer: VecDeque<Event>
}


impl InMemory {
    pub fn new() -> InMemory {
        InMemory {
           buffer: VecDeque::new()
        }
    }

    fn insert(&mut self, mut events: Vec<Event>) -> () {
        loop {
            if events.is_empty() {
                break
            } else {
                match events.pop() {
                    Some(event) => {
                        self.buffer.push_front(event)
                    },
                    None => { break }
                }
            }
        }
    }

    fn insert_interleaved(&mut self, mut  events: Vec<Event>) -> () {
        let oldest_event = match events.get(0).cloned() {
            Some(event) => {event},
            None => { return }
        };

        events.reverse();
        let mut index = 0;

        // Find location to start inserting
        loop {
            match self.buffer.get(index) {
                Some(event) => {
                    if event.timestamp > oldest_event.timestamp {
                        index += 1;
                    } else {
                        break
                    }
                    
                },
                None => { break }
            }
        }

        // Insert events
        loop {
             match events.pop() {
                Some(event) => {
                    match self.buffer.get(index) {
                        Some(old_event) => {
                            if event.timestamp < old_event.timestamp {
                                index -= 1;
                            } else {
                                self.buffer.insert(index, event);
                                index -= 1;
                            }
                            
                        },
                        None => {
                            self.buffer.push_front(event);
                        }
                    }
                },
                None => { break }
            }
        }
    }
}

impl Store for InMemory {
    fn add_events(&mut self, events: Vec<Event>) -> () {
        if events.len() == 0 { return }

        if self.buffer.is_empty() {
            self.insert(events);
        } else {
            self.insert_interleaved(events);
        }
    }

    fn get_window(&self, win_len_ms: i64) -> Vec<Event> {
        let mut window = Vec::new();
        let now: DateTime<Utc> = Utc::now();

        loop {
            match self.buffer.front() {
                Some(event) => {
                    let delta = now - event.timestamp;
                    if delta.num_milliseconds() > win_len_ms {
                        break
                    } else {
                         window.push(event.clone());
                    }
                },
                None => break
            }
        }

        return window;
    }

    fn get_window_of_n(&self, n: u64) -> Vec<Event> {
        let mut window = Vec::new();
        let mut index = 0;

        loop {
            if index <= n {
                match self.buffer.front() {
                    Some(event) => {
                        window.push(event.clone());
                        index += 1;
                    },
                    None => break
                }
            } else {
                break
            }
        }

        return window;
    }
}

