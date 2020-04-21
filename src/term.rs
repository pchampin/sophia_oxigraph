//! Conversion between Sophia and Oxigraph Terms.
use oxigraph::model::{
    BlankNode as OBlankNode, Literal as OLiteral, NamedNode, NamedOrBlankNode, Term as OTerm,
};
use sophia_term::blank_node::BlankNode as SBlankNode;
use sophia_term::iri::Iri as SIri;
use sophia_term::literal::Literal as SLiteral;
use sophia_term::{Term as STerm, TermData};
use std::io::Write;
use thiserror::Error;

lazy_static::lazy_static! {
    /// The xsd:string namespace
    pub static ref XSD_STRING: SIri<String> = SIri::new_unchecked("http://www.w3.org/2001/XMLSchema#string", true);
}

/// Trait for converting to Sophia blank nodes
pub trait AsSophiaBlankNode {
    /// Convert by simply borrowing the underlying text of self
    fn as_sophia_b_ref(&self) -> SBlankNode<&str>;
    /// Convert by copying the underlying text of self
    fn as_sophia_b<TD>(&self) -> SBlankNode<TD>
    where
        TD: TermData + for<'x> From<&'x str>;
    /// Convert by consuming the underlying text of self
    fn into_sophia_b<TD>(self) -> SBlankNode<TD>
    where
        TD: TermData + From<String>;
}

impl AsSophiaBlankNode for OBlankNode {
    fn as_sophia_b_ref(&self) -> SBlankNode<&str> {
        SBlankNode::new_unchecked(self.as_str())
    }
    fn as_sophia_b<TD>(&self) -> SBlankNode<TD>
    where
        TD: TermData + for<'x> From<&'x str>,
    {
        SBlankNode::new_unchecked(self.as_str())
    }
    fn into_sophia_b<TD>(self) -> SBlankNode<TD>
    where
        TD: TermData + From<String>,
    {
        SBlankNode::new_unchecked(self.as_str().to_string())
    }
}

/// Trait for converting to Sophia IRI
pub trait AsSophiaIri {
    /// Convert by simply borrowing the underlying text of self
    fn as_sophia_i_ref(&self) -> SIri<&str>;
    /// Convert by copying the underlying text of self
    fn as_sophia_i<TD>(&self) -> SIri<TD>
    where
        TD: TermData + for<'x> From<&'x str>;
    /// Convert by consuming the underlying text of self
    fn into_sophia_i<TD>(self) -> SIri<TD>
    where
        TD: TermData + From<String>;
}

impl AsSophiaIri for NamedNode {
    fn as_sophia_i_ref(&self) -> SIri<&str> {
        SIri::new_unchecked(self.as_str(), true)
    }
    fn as_sophia_i<TD>(&self) -> SIri<TD>
    where
        TD: TermData + for<'x> From<&'x str>,
    {
        SIri::new_unchecked(self.as_str(), true)
    }
    fn into_sophia_i<TD>(self) -> SIri<TD>
    where
        TD: TermData + From<String>,
    {
        SIri::new_unchecked(self.into_string(), true)
    }
}

/// Trait for converting to Sophia LIteral
pub trait AsSophiaLiteral {
    /// Convert by simply borrowing the underlying text of self
    fn as_sophia_l_ref(&self) -> SLiteral<&str>;
    /// Convert by copying the underlying text of self
    fn as_sophia_l<TD>(&self) -> SLiteral<TD>
    where
        TD: TermData + for<'x> From<&'x str>;
    /// Convert by consuming the underlying text of self
    fn into_sophia_l<TD>(self) -> SLiteral<TD>
    where
        TD: TermData + From<String>;
}

impl AsSophiaLiteral for OLiteral {
    fn as_sophia_l_ref(&self) -> SLiteral<&str> {
        match self.language() {
            None => SLiteral::new_dt(self.value(), self.datatype().as_sophia_i_ref()),
            Some(tag) => SLiteral::new_lang_unchecked(self.value(), tag),
        }
    }
    fn as_sophia_l<TD>(&self) -> SLiteral<TD>
    where
        TD: TermData + for<'x> From<&'x str>,
    {
        match self.language() {
            None => SLiteral::new_dt(self.value(), self.datatype().as_sophia_i::<TD>()),
            Some(tag) => SLiteral::new_lang(self.value(), tag)
                .unwrap_or_else(|_| SLiteral::new_lang_unchecked(self.value(), "und")),
        }
    }
    fn into_sophia_l<TD>(self) -> SLiteral<TD>
    where
        TD: TermData + From<String>,
    {
        let (val, dt, lang) = self.destruct();
        match (dt, lang) {
            (None, None) => SLiteral::new_dt(val, XSD_STRING.clone()),
            (Some(dt), _) => SLiteral::new_dt(val, dt.into_sophia_i()),
            (_, Some(tag)) => SLiteral::new_lang_unchecked(val, tag),
        }
    }
}

/// Trait for converting to Sophia Term
pub trait AsSophiaTerm {
    /// Convert by simply borrowing the underlying text of self
    fn as_sophia_ref(&self) -> STerm<&str>;
    /// Convert by copying the underlying text of self
    fn as_sophia<TD>(&self) -> STerm<TD>
    where
        TD: TermData + for<'x> From<&'x str>;
    /// Convert by consuming the underlying text of self
    fn into_sophia<TD>(self) -> STerm<TD>
    where
        TD: TermData + From<String>;
}

impl AsSophiaTerm for OTerm {
    fn as_sophia_ref(&self) -> STerm<&str> {
        match self {
            OTerm::BlankNode(b) => STerm::BNode(b.as_sophia_b_ref()),
            OTerm::Literal(l) => STerm::Literal(l.as_sophia_l_ref()),
            OTerm::NamedNode(n) => STerm::Iri(n.as_sophia_i_ref()),
        }
    }
    fn as_sophia<TD>(&self) -> STerm<TD>
    where
        TD: TermData + for<'x> From<&'x str>,
    {
        match self {
            OTerm::BlankNode(b) => STerm::BNode(b.as_sophia_b()),
            OTerm::Literal(l) => STerm::Literal(l.as_sophia_l()),
            OTerm::NamedNode(n) => STerm::Iri(n.as_sophia_i()),
        }
    }
    fn into_sophia<TD>(self) -> STerm<TD>
    where
        TD: TermData + From<String>,
    {
        match self {
            OTerm::BlankNode(b) => STerm::BNode(b.into_sophia_b()),
            OTerm::Literal(l) => STerm::Literal(l.into_sophia_l()),
            OTerm::NamedNode(n) => STerm::Iri(n.into_sophia_i()),
        }
    }
}

impl AsSophiaTerm for NamedOrBlankNode {
    fn as_sophia_ref(&self) -> STerm<&str> {
        match self {
            NamedOrBlankNode::BlankNode(b) => STerm::BNode(b.as_sophia_b_ref()),
            NamedOrBlankNode::NamedNode(n) => STerm::Iri(n.as_sophia_i_ref()),
        }
    }
    fn as_sophia<TD>(&self) -> STerm<TD>
    where
        TD: TermData + for<'x> From<&'x str>,
    {
        match self {
            NamedOrBlankNode::BlankNode(b) => STerm::BNode(b.as_sophia_b()),
            NamedOrBlankNode::NamedNode(n) => STerm::Iri(n.as_sophia_i()),
        }
    }
    fn into_sophia<TD>(self) -> STerm<TD>
    where
        TD: TermData + From<String>,
    {
        match self {
            NamedOrBlankNode::BlankNode(b) => STerm::BNode(b.into_sophia_b()),
            NamedOrBlankNode::NamedNode(n) => STerm::Iri(n.into_sophia_i()),
        }
    }
}

impl AsSophiaTerm for NamedNode {
    fn as_sophia_ref(&self) -> STerm<&str> {
        STerm::Iri(self.as_sophia_i_ref())
    }
    fn as_sophia<TD>(&self) -> STerm<TD>
    where
        TD: TermData + for<'x> From<&'x str>,
    {
        STerm::Iri(self.as_sophia_i())
    }
    fn into_sophia<TD>(self) -> STerm<TD>
    where
        TD: TermData + From<String>,
    {
        STerm::Iri(self.into_sophia_i())
    }
}

/// Trait for converting to Oxigraph term
pub trait TryOxigraphize<T> {
    /// Convert to an Oxigraph term type
    fn try_oxigraphize(&self) -> Result<T, ConversionError>;
}

impl<TD: TermData> TryOxigraphize<OBlankNode> for SBlankNode<TD> {
    fn try_oxigraphize(&self) -> Result<OBlankNode, ConversionError> {
        let value = self.value();
        if let Ok(id) = u128::from_str_radix(&value, 16) {
            return Ok(OBlankNode::new_from_unique_id(id));
        }
        if value.len() <= 16 {
            let mut id = [0_u8; 16];
            write!(&mut id[..], "{}", value).unwrap();
            let id = unsafe { std::mem::transmute(id) };
            return Ok(OBlankNode::new_from_unique_id(id));
        }
        Err(ConversionError::IncompatibleBnodeId(
            self.value().to_string(),
        ))
    }
}

impl<TD: TermData> TryOxigraphize<NamedNode> for SIri<TD> {
    fn try_oxigraphize(&self) -> Result<NamedNode, ConversionError> {
        let value = self.value().to_string();
        if !self.is_absolute() {
            Err(ConversionError::RelativeIriRef(value))
        } else {
            Ok(NamedNode::new_unchecked(value))
        }
    }
}

impl<TD: TermData> TryOxigraphize<OLiteral> for SLiteral<TD> {
    fn try_oxigraphize(&self) -> Result<OLiteral, ConversionError> {
        let value = self.value().to_string();
        Ok(match self.lang() {
            None => OLiteral::new_typed_literal(value, self.dt().try_oxigraphize()?),
            Some(tag) => OLiteral::new_language_tagged_literal_unchecked(
                value,
                tag.as_ref().to_ascii_lowercase(),
            ),
        })
    }
}

impl<TD: TermData> TryOxigraphize<OTerm> for STerm<TD> {
    fn try_oxigraphize(&self) -> Result<OTerm, ConversionError> {
        match self {
            STerm::BNode(b) => Ok(OTerm::BlankNode(b.try_oxigraphize()?)),
            STerm::Iri(i) => Ok(OTerm::NamedNode(i.try_oxigraphize()?)),
            STerm::Literal(l) => Ok(OTerm::Literal(l.try_oxigraphize()?)),
            STerm::Variable(v) => Err(ConversionError::Variable(v.as_str().to_string())),
        }
    }
}

impl<TD: TermData> TryOxigraphize<NamedOrBlankNode> for STerm<TD> {
    fn try_oxigraphize(&self) -> Result<NamedOrBlankNode, ConversionError> {
        match self {
            STerm::BNode(b) => Ok(NamedOrBlankNode::BlankNode(b.try_oxigraphize()?)),
            STerm::Iri(i) => Ok(NamedOrBlankNode::NamedNode(i.try_oxigraphize()?)),
            STerm::Literal(l) => Err(ConversionError::Literal(l.value().to_string())),
            STerm::Variable(v) => Err(ConversionError::Variable(v.as_str().to_string())),
        }
    }
}

impl<TD: TermData> TryOxigraphize<NamedNode> for STerm<TD> {
    fn try_oxigraphize(&self) -> Result<NamedNode, ConversionError> {
        match self {
            STerm::BNode(b) => Err(ConversionError::BlankNode(b.as_str().to_string())),
            STerm::Iri(i) => Ok(i.try_oxigraphize()?),
            STerm::Literal(l) => Err(ConversionError::Literal(l.value().to_string())),
            STerm::Variable(v) => Err(ConversionError::Variable(v.as_str().to_string())),
        }
    }
}

/// This error is raised when a Sophia term can not be converted to Oxigraph
#[derive(Debug, Error)]
pub enum ConversionError {
    /// The sophia term is a blank node used in predicate position
    #[error("Oxigraph does not support blank node in predicate position '{0}'")]
    BlankNode(String),
    /// Incompatible blank-node identifier
    #[error("Oxigraph does not support this bnode ID '{0}'")]
    IncompatibleBnodeId(String),
    /// The sophia term is a literal used in subject or predicate position
    #[error("Oxigraph only supports literals in object position '{0}'")]
    Literal(String),
    /// The IRI reference is relative
    #[error("Oxigraph does not support relatife IRIrefs '{0}'")]
    RelativeIriRef(String),
    /// The sophia term is a variable
    #[error("Oxigraph does not variables as terms '{0}'")]
    Variable(String),
}
