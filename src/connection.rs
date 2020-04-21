//! Sophia Dataset implementation for Oxigraph RepositoryConnection
use crate::quad::QuadBridge;
use crate::term::{AsSophiaTerm, ConversionError, TryOxigraphize};
use oxigraph::model::{NamedNode, NamedOrBlankNode, Quad as OQuad, Term as OTerm};
use oxigraph::sparql::{PreparedQuery, QueryOptions, QueryResult};
use oxigraph::{Error as OxigraphError, RepositoryConnection};
use sophia::dataset::{DQuadSource, DResult, DResultTermSet, Dataset, MDResult, MutableDataset};
use sophia::quad::streaming_mode::*;
use sophia_term::{Term, TermData};
use std::collections::HashSet;
use std::iter::empty;
use thiserror::Error;

/// Expose an Oxigraph Connection as a Sophia Dataset
#[derive(Clone, Debug, Default)]
pub struct SophiaConnection<C: RepositoryConnection>(C);

impl<C> SophiaConnection<C>
where
    C: RepositoryConnection,
{
    /// Wrap `conn` as a Sophia Dataset
    #[inline]
    pub fn new(conn: C) -> Self {
        SophiaConnection(conn)
    }

    /// Borrow underlying Oxigraph connection
    #[inline]
    pub fn as_oxi(&self) -> &C {
        &self.0
    }

    /// Borrow underlying Oxigraph connection mutably
    #[inline]
    pub fn as_oxi_mut(&mut self) -> &mut C {
        &mut self.0
    }
}

impl<C> Dataset for SophiaConnection<C>
where
    C: RepositoryConnection,
{
    type Quad = ByValue<QuadBridge>;
    type Error = OxigraphError;

    fn quads(&self) -> DQuadSource<Self> {
        Box::new(
            self.0
                .quads_for_pattern(None, None, None, None)
                .map(|r| r.map(|q| StreamedQuad::by_value(QuadBridge::new(q)))),
        )
    }

    fn quads_with_s<'s, T>(&'s self, s: &'s Term<T>) -> DQuadSource<'s, Self>
    where
        T: TermData,
    {
        match s.try_oxigraphize() {
            Ok(s) => Box::new(
                self.0
                    .quads_for_pattern(Some(&s), None, None, None)
                    .map(bridge),
            ),
            Err(_) => Box::new(empty()),
        }
    }

    fn quads_with_p<'s, T>(&'s self, p: &'s Term<T>) -> DQuadSource<'s, Self>
    where
        T: TermData,
    {
        match p.try_oxigraphize() {
            Ok(p) => Box::new(
                self.0
                    .quads_for_pattern(None, Some(&p), None, None)
                    .map(bridge),
            ),
            Err(_) => Box::new(empty()),
        }
    }

    fn quads_with_o<'s, T>(&'s self, o: &'s Term<T>) -> DQuadSource<'s, Self>
    where
        T: TermData,
    {
        match o.try_oxigraphize() {
            Ok(o) => Box::new(
                self.0
                    .quads_for_pattern(None, None, Some(&o), None)
                    .map(bridge),
            ),
            Err(_) => Box::new(empty()),
        }
    }

    fn quads_with_g<'s, T>(&'s self, g: Option<&'s Term<T>>) -> DQuadSource<'s, Self>
    where
        T: TermData,
    {
        match try_oxi_graphname(g) {
            Ok(g) => Box::new(
                self.0
                    .quads_for_pattern(None, None, None, Some(g.as_ref()))
                    .map(bridge),
            ),
            Err(_) => Box::new(empty()),
        }
    }

    fn quads_with_sp<'s, T, U>(&'s self, s: &'s Term<T>, p: &'s Term<U>) -> DQuadSource<'s, Self>
    where
        T: TermData,
        U: TermData,
    {
        match (s.try_oxigraphize(), p.try_oxigraphize()) {
            (Ok(s), Ok(p)) => Box::new(
                self.0
                    .quads_for_pattern(Some(&s), Some(&p), None, None)
                    .map(bridge),
            ),
            _ => Box::new(empty()),
        }
    }

    fn quads_with_so<'s, T, U>(&'s self, s: &'s Term<T>, o: &'s Term<U>) -> DQuadSource<'s, Self>
    where
        T: TermData,
        U: TermData,
    {
        match (s.try_oxigraphize(), o.try_oxigraphize()) {
            (Ok(s), Ok(o)) => Box::new(
                self.0
                    .quads_for_pattern(Some(&s), None, Some(&o), None)
                    .map(bridge),
            ),
            _ => Box::new(empty()),
        }
    }

    fn quads_with_sg<'s, T, U>(
        &'s self,
        s: &'s Term<T>,
        g: Option<&'s Term<U>>,
    ) -> DQuadSource<'s, Self>
    where
        T: TermData,
        U: TermData,
    {
        match (s.try_oxigraphize(), try_oxi_graphname(g)) {
            (Ok(s), Ok(g)) => Box::new(
                self.0
                    .quads_for_pattern(Some(&s), None, None, Some(g.as_ref()))
                    .map(bridge),
            ),
            _ => Box::new(empty()),
        }
    }

    fn quads_with_po<'s, T, U>(&'s self, p: &'s Term<T>, o: &'s Term<U>) -> DQuadSource<'s, Self>
    where
        T: TermData,
        U: TermData,
    {
        match (p.try_oxigraphize(), o.try_oxigraphize()) {
            (Ok(p), Ok(o)) => Box::new(
                self.0
                    .quads_for_pattern(None, Some(&p), Some(&o), None)
                    .map(bridge),
            ),
            _ => Box::new(empty()),
        }
    }

    fn quads_with_pg<'s, T, U>(
        &'s self,
        p: &'s Term<T>,
        g: Option<&'s Term<U>>,
    ) -> DQuadSource<'s, Self>
    where
        T: TermData,
        U: TermData,
    {
        match (p.try_oxigraphize(), try_oxi_graphname(g)) {
            (Ok(p), Ok(g)) => Box::new(
                self.0
                    .quads_for_pattern(None, Some(&p), None, Some(g.as_ref()))
                    .map(bridge),
            ),
            _ => Box::new(empty()),
        }
    }

    fn quads_with_og<'s, T, U>(
        &'s self,
        o: &'s Term<T>,
        g: Option<&'s Term<U>>,
    ) -> DQuadSource<'s, Self>
    where
        T: TermData,
        U: TermData,
    {
        match (o.try_oxigraphize(), try_oxi_graphname(g)) {
            (Ok(o), Ok(g)) => Box::new(
                self.0
                    .quads_for_pattern(None, None, Some(&o), Some(g.as_ref()))
                    .map(bridge),
            ),
            _ => Box::new(empty()),
        }
    }

    fn quads_with_spo<'s, T, U, V>(
        &'s self,
        s: &'s Term<T>,
        p: &'s Term<U>,
        o: &'s Term<V>,
    ) -> DQuadSource<'s, Self>
    where
        T: TermData,
        U: TermData,
        V: TermData,
    {
        match (
            s.try_oxigraphize(),
            p.try_oxigraphize(),
            o.try_oxigraphize(),
        ) {
            (Ok(s), Ok(p), Ok(o)) => Box::new(
                self.0
                    .quads_for_pattern(Some(&s), Some(&p), Some(&o), None)
                    .map(bridge),
            ),
            _ => Box::new(empty()),
        }
    }

    fn quads_with_spg<'s, T, U, V>(
        &'s self,
        s: &'s Term<T>,
        p: &'s Term<U>,
        g: Option<&'s Term<V>>,
    ) -> DQuadSource<'s, Self>
    where
        T: TermData,
        U: TermData,
        V: TermData,
    {
        match (
            s.try_oxigraphize(),
            p.try_oxigraphize(),
            try_oxi_graphname(g),
        ) {
            (Ok(s), Ok(p), Ok(g)) => Box::new(
                self.0
                    .quads_for_pattern(Some(&s), Some(&p), None, Some(g.as_ref()))
                    .map(bridge),
            ),
            _ => Box::new(empty()),
        }
    }

    fn quads_with_sog<'s, T, U, V>(
        &'s self,
        s: &'s Term<T>,
        o: &'s Term<U>,
        g: Option<&'s Term<V>>,
    ) -> DQuadSource<'s, Self>
    where
        T: TermData,
        U: TermData,
        V: TermData,
    {
        match (
            s.try_oxigraphize(),
            o.try_oxigraphize(),
            try_oxi_graphname(g),
        ) {
            (Ok(s), Ok(o), Ok(g)) => Box::new(
                self.0
                    .quads_for_pattern(Some(&s), None, Some(&o), Some(g.as_ref()))
                    .map(bridge),
            ),
            _ => Box::new(empty()),
        }
    }

    fn quads_with_pog<'s, T, U, V>(
        &'s self,
        p: &'s Term<T>,
        o: &'s Term<U>,
        g: Option<&'s Term<V>>,
    ) -> DQuadSource<'s, Self>
    where
        T: TermData,
        U: TermData,
        V: TermData,
    {
        match (
            p.try_oxigraphize(),
            o.try_oxigraphize(),
            try_oxi_graphname(g),
        ) {
            (Ok(p), Ok(o), Ok(g)) => Box::new(
                self.0
                    .quads_for_pattern(None, Some(&p), Some(&o), Some(g.as_ref()))
                    .map(bridge),
            ),
            _ => Box::new(empty()),
        }
    }

    fn quads_with_spog<'s, T, U, V, W>(
        &'s self,
        s: &'s Term<T>,
        p: &'s Term<U>,
        o: &'s Term<V>,
        g: Option<&'s Term<W>>,
    ) -> DQuadSource<'s, Self>
    where
        T: TermData,
        U: TermData,
        V: TermData,
        W: TermData,
    {
        match (
            s.try_oxigraphize(),
            p.try_oxigraphize(),
            o.try_oxigraphize(),
            try_oxi_graphname(g),
        ) {
            (Ok(s), Ok(p), Ok(o), Ok(g)) => Box::new(
                self.0
                    .quads_for_pattern(Some(&s), Some(&p), Some(&o), Some(g.as_ref()))
                    .map(bridge),
            ),
            _ => Box::new(empty()),
        }
    }

    fn contains<T, U, V, W>(
        &self,
        s: &Term<T>,
        p: &Term<U>,
        o: &Term<V>,
        g: Option<&Term<W>>,
    ) -> DResult<Self, bool>
    where
        T: TermData,
        U: TermData,
        V: TermData,
        W: TermData,
    {
        match (
            TryOxigraphize::<NamedOrBlankNode>::try_oxigraphize(s),
            TryOxigraphize::<NamedNode>::try_oxigraphize(p),
            TryOxigraphize::<OTerm>::try_oxigraphize(o),
            try_oxi_graphname(g),
        ) {
            (Ok(s), Ok(p), Ok(o), Ok(g)) => self.0.contains(&OQuad::new(s, p, o, g)),
            _ => Ok(false),
        }
    }

    fn subjects(&self) -> DResultTermSet<Self> {
        let q = self.0.prepare_query(
            "SELECT DISTINCT ?s {{?s ?p ?o} UNION { GRAPH ?g {?s ?p ?o}}}",
            QueryOptions::default(),
        )?;
        let r = q.exec()?;
        sparql_result_as_term_set(r)
    }

    fn predicates(&self) -> DResultTermSet<Self> {
        let q = self.0.prepare_query(
            "SELECT DISTINCT ?p {{?s ?p ?o} UNION { GRAPH ?g {?s ?p ?o}}}",
            QueryOptions::default(),
        )?;
        let r = q.exec()?;
        sparql_result_as_term_set(r)
    }

    fn objects(&self) -> DResultTermSet<Self> {
        let q = self.0.prepare_query(
            "SELECT DISTINCT ?o {{?s ?p ?o} UNION { GRAPH ?g {?s ?p ?o}}}",
            QueryOptions::default(),
        )?;
        let r = q.exec()?;
        sparql_result_as_term_set(r)
    }

    fn graph_names(&self) -> DResultTermSet<Self> {
        let q = self.0.prepare_query(
            "SELECT DISTINCT ?g {GRAPH ?g {?s ?p ?o}}",
            QueryOptions::default(),
        )?;
        let r = q.exec()?;
        sparql_result_as_term_set(r)
    }

    fn iris(&self) -> DResultTermSet<Self> {
        let q = self.0.prepare_query("SELECT DISTINCT ?iri {{?iri ?p ?o} UNION {?s ?iri ?o} UNION {?s ?p ?iri} UNION {GRAPH ?iri {?s ?p ?o}} UNION {GRAPH ?s {?iri ?p ?o}} UNION {GRAPH ?g {?s ?iri ?o}} UNION {GRAPH ?g {?s ?p ?iri}} FILTER isIRI(?iri)}", QueryOptions::default())?;
        let r = q.exec()?;
        sparql_result_as_term_set(r)
    }

    fn bnodes(&self) -> DResultTermSet<Self> {
        let q = self.0.prepare_query("SELECT DISTINCT ?bn {{?bn ?p ?o} UNION {?s ?p ?bn} UNION {GRAPH ?bn {?s ?p ?o}} UNION {GRAPH ?s {?bn ?p ?o}} UNION {GRAPH ?g {?s ?p ?bn}} FILTER isBlank(?bn)}", QueryOptions::default())?;
        let r = q.exec()?;
        sparql_result_as_term_set(r)
    }

    fn literals(&self) -> DResultTermSet<Self> {
        let q = self.0.prepare_query("SELECT DISTINCT ?lit {{?s ?p ?lit} UNION { GRAPH ?g {?s ?p ?lit}} FILTER isLiteral(?lit)}", QueryOptions::default())?;
        let r = q.exec()?;
        sparql_result_as_term_set(r)
    }

    fn variables(&self) -> DResultTermSet<Self> {
        Ok(HashSet::new())
    }
}

impl<C> MutableDataset for SophiaConnection<C>
where
    C: RepositoryConnection,
{
    type MutationError = MutationError;

    fn insert<T, U, V, W>(
        &mut self,
        s: &Term<T>,
        p: &Term<U>,
        o: &Term<V>,
        g: Option<&Term<W>>,
    ) -> MDResult<Self, bool>
    where
        T: TermData,
        U: TermData,
        V: TermData,
        W: TermData,
    {
        let s: NamedOrBlankNode = s.try_oxigraphize()?;
        let p: NamedNode = p.try_oxigraphize()?;
        let o: OTerm = o.try_oxigraphize()?;
        let g = try_oxi_graphname(g)?;
        self.0.insert(&OQuad::new(s, p, o, g))?;
        Ok(true) // TODO: this may not be accurate
    }

    fn remove<T, U, V, W>(
        &mut self,
        s: &Term<T>,
        p: &Term<U>,
        o: &Term<V>,
        g: Option<&Term<W>>,
    ) -> MDResult<Self, bool>
    where
        T: TermData,
        U: TermData,
        V: TermData,
        W: TermData,
    {
        let s: Result<NamedOrBlankNode, _> = s.try_oxigraphize();
        let p: Result<NamedNode, _> = p.try_oxigraphize();
        let o: Result<OTerm, _> = o.try_oxigraphize();
        let g = try_oxi_graphname(g);
        if let (Ok(s), Ok(p), Ok(o), Ok(g)) = (s, p, o, g) {
            self.0.remove(&OQuad::new(s, p, o, g))?;
            Ok(true) // TODO: this may not be accurate
        } else {
            Ok(false)
        }
    }

    // TODO implement other methods (using SPARQL under the hood)
}

#[inline]
/// Shortcut function to convert Oxigraph Quad to Sophia Quad
fn bridge<'a>(
    r: Result<OQuad, OxigraphError>,
) -> Result<StreamedQuad<'a, ByValue<QuadBridge>>, OxigraphError> {
    r.map(|q| StreamedQuad::by_value(QuadBridge::new(q)))
}

#[inline]
/// Shortcut function to convert Sophia graph name to Oxigraph graph name
fn try_oxi_graphname<T: TermData>(
    g: Option<&Term<T>>,
) -> Result<Option<NamedOrBlankNode>, ConversionError> {
    g.map(|g| g.try_oxigraphize()).transpose()
}

#[inline]
/// Convert the result of a SPARQL query into a term set
///
/// # Precondition
/// + the query must be a SELECT query with a single selected variable
/// + it must not produce NULL results
fn sparql_result_as_term_set(r: QueryResult) -> Result<HashSet<Term<String>>, OxigraphError> {
    if let QueryResult::Bindings(b) = r {
        b.into_values_iter()
            .map(|r| r.map(|mut v| v.pop().unwrap().unwrap().as_sophia()))
            .collect()
    } else {
        unreachable!()
    }
}

/// Mutation error for the Oxigraph-to-Sophia adapter
#[derive(Debug, Error)]
pub enum MutationError {
    /// Error from Oxigraph
    #[error("{source}")]
    Oxigraph {
        /// The source of this error
        #[from]
        source: OxigraphError,
    },
    /// Error from term conversion
    #[error("Conversion: {source}")]
    Conversion {
        /// The source of this error
        #[from]
        source: ConversionError,
    },
}

impl From<std::convert::Infallible> for MutationError {
    fn from(_: std::convert::Infallible) -> Self {
        unreachable!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use oxigraph::{MemoryRepository, Repository};
    use sophia_term::matcher::ANY;

    lazy_static::lazy_static! {
        pub static ref REP: MemoryRepository = MemoryRepository::default();
    }

    type MemRepRef = &'static MemoryRepository;
    type ConDataset = SophiaConnection<<MemRepRef as Repository>::Connection>;

    #[allow(dead_code)]
    fn make_dataset() -> ConDataset {
        let mut conn = SophiaConnection(REP.connection().unwrap());
        conn.remove_matching(&ANY, &ANY, &ANY, &ANY).unwrap();
        conn
    }

    // These tests only work if options "-- --test-threads 1" is provided to cargo test,
    // because they share a single repository REP.
    //sophia::test_dataset_impl!(auto, ConDataset, false, make_dataset, false);

    // Anyway, they are not strictly required:
    // SophiaConnection is tested trough SophiaRepository,
    // which simply delegates all Dataset methods to the underlying SophiaConnection.
}
