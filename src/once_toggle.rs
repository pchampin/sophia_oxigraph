//! Utility type for lazy initialization.
//!
//! A OnceToggle has two states.
//!
//! In state 1, it contains an immutable value of type T,
//! and the only thing it can do is to toggle to state 2.
//!
//! Togglin is performed by a function mapping T to U.
//!
//! In state 2, it contains a mutable value of type U,
//! which can be borrowed (immutably or mutably) without restricyions
//! (unlike RefCells).
//!
//! # Todo
//!
//! * improve memory layout: at any time, we will only store T or U.
//!
//! * implement `sync` version.

use once_cell::unsync::OnceCell;
use std::cell::RefCell;

/// See [module documentation](./index.html)
pub struct OnceToggle<T, U> {
    state1: RefCell<Option<T>>,
    state2: OnceCell<U>,
}

impl<T, U> OnceToggle<T, U> {
    /// Create a new OnceToggle with the given value for state 1.
    pub fn new(value: T) -> Self {
        OnceToggle {
            state1: RefCell::new(Some(value)),
            state2: OnceCell::new(),
        }
    }
    /// Return the state (1 or 2) of this OnceToggle.
    pub fn state(&self) -> u8 {
        match self.state2.get() {
            None => 1,
            Some(_) => 2,
        }
    }

    /// Try toggling to state 2, using the given function.
    ///
    /// # Pre-conditions
    ///
    /// This OnceToggle must still be in state 1.
    ///
    /// # Post-condition
    ///
    /// If function `f` returns an error,
    /// this OnceToggle is in an inconsistent state,
    /// and should not be used again.
    pub fn try_toggle<F, E>(&self, f: F) -> Result<(), E>
    where
        F: FnOnce(T) -> Result<U, E>,
    {
        let state1 = self.state1.borrow_mut().take().unwrap();
        self.state2.set(f(state1)?).map_err(|_| ()).unwrap();
        Ok(())
    }

    /// Toggling to state 2, using the given function.
    ///
    /// # Pre-conditions
    ///
    /// This OnceToggle must still be in state 1.
    pub fn toggle<F>(&self, f: F)
    where
        F: FnOnce(T) -> U,
    {
        let state1 = self.state1.borrow_mut().take().unwrap();
        self.state2.set(f(state1)).map_err(|_| ()).unwrap();
    }

    /// Borrow immutably the state 2 value of this OnceToggle.
    ///
    /// # Pre-conditions
    ///
    /// This OnceToggle must be in state 2.
    pub fn get(&self) -> &U {
        self.state2.get().unwrap()
    }

    /// Borrow mutably the state 2 value of this OnceToggle.
    ///
    /// # Pre-conditions
    ///
    /// This OnceToggle must be in state 2.
    pub fn get_mut(&mut self) -> &mut U {
        self.state2.get_mut().unwrap()
    }

    /// Unwraps the state 2 value of this OnceToggle.
    ///
    /// # Pre-conditions
    ///
    /// This OnceToggle must be in state 2.
    pub fn unwrap(self) -> U {
        self.state2.into_inner().unwrap()
    }

    /// Borrow immutably the state 2 value of this OnceToggle,
    /// toggling if necessary.
    ///
    /// # Post-condition
    ///
    /// If function `f` returns an error,
    /// this OnceToggle is in an inconsistent state,
    /// and should not be used again.
    pub fn get_or_try_toggle<F, E>(&self, f: F) -> Result<&U, E>
    where
        F: FnOnce(T) -> Result<U, E>,
    {
        let state2 = self.state2.get();
        if let Some(ret) = state2 {
            Ok(ret)
        } else {
            let state1 = self.state1.borrow_mut().take().unwrap();
            self.state2.get_or_try_init(move || f(state1))
        }
    }

    /// Borrow immutably the state 2 value of this OnceToggle,
    /// toggling if necessary.
    pub fn get_or_toggle<F>(&self, f: F) -> &U
    where
        F: FnOnce(T) -> U,
    {
        let state2 = self.state2.get();
        if let Some(ret) = state2 {
            ret
        } else {
            let state1 = self.state1.borrow_mut().take().unwrap();
            self.state2.get_or_init(move || f(state1))
        }
    }
}
