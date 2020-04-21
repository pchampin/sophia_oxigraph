//! Sophia Dataset implementation for Oxigraph Repository
use crate::connection::{MutationError, SophiaConnection};
use crate::quad::QuadBridge;
use oxigraph::{Error as OxigraphError, Repository};
use sophia::dataset::{DQuadSource, DResult, DResultTermSet, Dataset, MDResult, MutableDataset};
use sophia::quad::streaming_mode::*;
use sophia_term::matcher::{GraphNameMatcher, TermMatcher};
use sophia_term::{Term, TermData};
use std::mem::transmute;
use std::pin::Pin;

type SoCx<'a, R> = SophiaConnection<<&'a R as Repository>::Connection>;

/// Expose an Oxigraph Connection as a Sophia Dataset
pub struct SophiaRepository<R>
where
    R: 'static,
    for<'x> &'x R: Repository,
{
    repo: R,
    conn: Option<SoCx<'static, R>>,
}

impl<R> SophiaRepository<R>
where
    R: 'static,
    for<'x> &'x R: Repository,
{
    /// Wrap `repo` as a Sophia Dataset
    #[inline]
    pub fn new(repo: R) -> Result<Pin<Box<Self>>, OxigraphError> {
        let mut pinned = Box::pin(SophiaRepository { repo, conn: None });
        unsafe {
            let sr = Pin::get_unchecked_mut(Pin::as_mut(&mut pinned));
            let repo: &'static R = transmute(&sr.repo);
            sr.conn = Some(SoCx::new(repo.connection()?));
        }
        Ok(pinned)
    }

    /// Borrow underlying Oxigraph repository
    #[inline]
    pub fn as_oxi(&self) -> &R {
        &self.repo
    }

    /// Get a SophiaConnection from the underlying repository
    #[inline]
    pub fn connection(&self) -> &SoCx<R> {
        unsafe { transmute(self.conn.as_ref().unwrap()) }
    }

    /// Get a SophiaConnection from the underlying repository
    #[inline]
    pub fn fresh_connection(&self) -> Result<SoCx<R>, OxigraphError> {
        Ok(SoCx::new(self.repo.connection()?))
    }
}

impl<R> Dataset for Pin<Box<SophiaRepository<R>>>
where
    for<'x> &'x R: Repository,
{
    type Quad = ByValue<QuadBridge>;
    type Error = OxigraphError;

    #[inline]
    fn quads(&self) -> DQuadSource<Self> {
        self.connection().quads()
    }
    #[inline]
    fn quads_with_s<'s, T>(&'s self, s: &'s Term<T>) -> DQuadSource<'s, Self>
    where
        T: TermData,
    {
        self.connection().quads_with_s(s)
    }
    #[inline]
    fn quads_with_p<'s, T>(&'s self, p: &'s Term<T>) -> DQuadSource<'s, Self>
    where
        T: TermData,
    {
        self.connection().quads_with_p(p)
    }
    #[inline]
    fn quads_with_o<'s, T>(&'s self, o: &'s Term<T>) -> DQuadSource<'s, Self>
    where
        T: TermData,
    {
        self.connection().quads_with_o(o)
    }
    #[inline]
    fn quads_with_g<'s, T>(&'s self, g: Option<&'s Term<T>>) -> DQuadSource<'s, Self>
    where
        T: TermData,
    {
        self.connection().quads_with_g(g)
    }
    #[inline]
    fn quads_with_sp<'s, T, U>(&'s self, s: &'s Term<T>, p: &'s Term<U>) -> DQuadSource<'s, Self>
    where
        T: TermData,
        U: TermData,
    {
        self.connection().quads_with_sp(s, p)
    }
    #[inline]
    fn quads_with_so<'s, T, U>(&'s self, s: &'s Term<T>, o: &'s Term<U>) -> DQuadSource<'s, Self>
    where
        T: TermData,
        U: TermData,
    {
        self.connection().quads_with_so(s, o)
    }
    #[inline]
    fn quads_with_sg<'s, T, U>(
        &'s self,
        s: &'s Term<T>,
        g: Option<&'s Term<U>>,
    ) -> DQuadSource<'s, Self>
    where
        T: TermData,
        U: TermData,
    {
        self.connection().quads_with_sg(s, g)
    }
    #[inline]
    fn quads_with_po<'s, T, U>(&'s self, p: &'s Term<T>, o: &'s Term<U>) -> DQuadSource<'s, Self>
    where
        T: TermData,
        U: TermData,
    {
        self.connection().quads_with_po(p, o)
    }
    #[inline]
    fn quads_with_pg<'s, T, U>(
        &'s self,
        p: &'s Term<T>,
        g: Option<&'s Term<U>>,
    ) -> DQuadSource<'s, Self>
    where
        T: TermData,
        U: TermData,
    {
        self.connection().quads_with_pg(p, g)
    }
    #[inline]
    fn quads_with_og<'s, T, U>(
        &'s self,
        o: &'s Term<T>,
        g: Option<&'s Term<U>>,
    ) -> DQuadSource<'s, Self>
    where
        T: TermData,
        U: TermData,
    {
        self.connection().quads_with_og(o, g)
    }
    #[inline]
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
        self.connection().quads_with_spo(s, p, o)
    }
    #[inline]
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
        self.connection().quads_with_spg(s, p, g)
    }
    #[inline]
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
        self.connection().quads_with_sog(s, o, g)
    }
    #[inline]
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
        self.connection().quads_with_pog(p, o, g)
    }
    #[inline]
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
        self.connection().quads_with_spog(s, p, o, g)
    }
    #[inline]
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
        self.connection().contains(s, p, o, g)
    }
    #[inline]
    fn quads_matching<'s, S, P, O, G>(
        &'s self,
        ms: &'s S,
        mp: &'s P,
        mo: &'s O,
        mg: &'s G,
    ) -> DQuadSource<'s, Self>
    where
        S: TermMatcher + ?Sized,
        P: TermMatcher + ?Sized,
        O: TermMatcher + ?Sized,
        G: GraphNameMatcher + ?Sized,
    {
        self.connection().quads_matching(ms, mp, mo, mg)
    }
    #[inline]
    fn subjects(&self) -> DResultTermSet<Self> {
        self.connection().subjects()
    }
    #[inline]
    fn predicates(&self) -> DResultTermSet<Self> {
        self.connection().predicates()
    }
    #[inline]
    fn objects(&self) -> DResultTermSet<Self> {
        self.connection().objects()
    }
    #[inline]
    fn graph_names(&self) -> DResultTermSet<Self> {
        self.connection().graph_names()
    }
    #[inline]
    fn iris(&self) -> DResultTermSet<Self> {
        self.connection().iris()
    }
    #[inline]
    fn bnodes(&self) -> DResultTermSet<Self> {
        self.connection().bnodes()
    }
    #[inline]
    fn literals(&self) -> DResultTermSet<Self> {
        self.connection().literals()
    }
    #[inline]
    fn variables(&self) -> DResultTermSet<Self> {
        self.connection().variables()
    }
}

impl<R> MutableDataset for Pin<Box<SophiaRepository<R>>>
where
    for<'x> &'x R: Repository,
{
    type MutationError = MutationError;
    #[inline]
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
        self.fresh_connection()?.insert(s, p, o, g)
    }
    #[inline]
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
        self.fresh_connection()?.remove(s, p, o, g)
    }

    // TODO implement other methods (relaying to SophiaConnection)
}

#[cfg(test)]
mod test {
    use super::*;
    use oxigraph::MemoryRepository;

    type SopMemRepo = Pin<Box<SophiaRepository<MemoryRepository>>>;

    fn make_repo() -> SopMemRepo {
        SophiaRepository::new(MemoryRepository::default()).unwrap()
    }

    sophia::test_dataset_impl!(auto, SopMemRepo, false, make_repo, false);
}
