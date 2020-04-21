//! Sophia Quad implementation of Oxigraph Quad
//!
//! TODO: this is a straighforward implementation,
//! which might be improved in term of CPU- and memory-efficiency.

use crate::once_toggle::OnceToggle;
use crate::term::*;
use oxigraph::model::{NamedNode, NamedOrBlankNode, Quad as OQuad, Term as OTerm};
use sophia::quad::Quad as SQuad;
use sophia_term::Term as STerm;

/// Wraps an Oxigraph Quad into a Sophia Quad
pub struct QuadBridge {
    s: OnceToggle<NamedOrBlankNode, STerm<String>>,
    p: OnceToggle<NamedNode, STerm<String>>,
    o: OnceToggle<OTerm, STerm<String>>,
    g: Option<OnceToggle<NamedOrBlankNode, STerm<String>>>,
}

impl QuadBridge {
    /// Construct QuadBridge around Oxigraph Quad
    pub fn new(q: OQuad) -> QuadBridge {
        let (subj, pred, obj, graph) = q.destruct();
        QuadBridge {
            s: OnceToggle::new(subj),
            p: OnceToggle::new(pred),
            o: OnceToggle::new(obj),
            g: graph.map(OnceToggle::new),
        }
    }
}

impl SQuad for QuadBridge {
    type TermData = String;
    fn s(&self) -> &STerm<String> {
        self.s.get_or_toggle(AsSophiaTerm::into_sophia)
    }
    fn p(&self) -> &STerm<String> {
        self.p.get_or_toggle(AsSophiaTerm::into_sophia)
    }
    fn o(&self) -> &STerm<String> {
        self.o.get_or_toggle(AsSophiaTerm::into_sophia)
    }
    fn g(&self) -> Option<&STerm<String>> {
        self.g
            .as_ref()
            .map(|g| g.get_or_toggle(AsSophiaTerm::into_sophia))
    }
}
