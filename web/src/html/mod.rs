//! HTML elements as Rust types
//!
//! Each HTML element is represented by its own type, which knows how to encode itself. The children
//! of an element follow its content model: a child that must be present is passed to the
//! constructor, while other children are appended with `child()`, which accepts anything that
//! converts into the element's content category, like [`FlowContent`]. Text and attribute values
//! are escaped when rendered; [`Unescaped`] is the only way to emit markup verbatim.

use std::borrow::Cow;

mod generated;
pub(crate) use generated::*;

/// Builds an HTML representation of a model
pub(crate) trait View {
    /// The HTML type used to represent the model, which may borrow from it
    type Output<'a>
    where
        Self: 'a;

    /// Builds the HTML representation of `self`
    fn view(&self) -> Self::Output<'_>;
}

/// Encodes a value as HTML
pub(crate) trait Encode {
    /// Appends the HTML encoding of `self` to `out`
    fn encode(&self, out: &mut String);

    fn to_string(&self) -> String {
        let mut out = String::new();
        self.encode(&mut out);
        out
    }
}

/// Setters for the global attributes, which apply to every element
pub(crate) trait GlobalAttributes<'a>: Sized {
    /// Adds a class to the `class` attribute
    fn class(mut self, class: impl Into<Cow<'a, str>>) -> Self {
        self.attributes_mut().add_class(class.into());
        self
    }

    /// Sets the `title` attribute
    fn title(mut self, value: impl Into<Cow<'a, str>>) -> Self {
        self.attributes_mut().set("title", value.into());
        self
    }

    /// Sets the `style` attribute
    fn style(mut self, value: impl Into<Cow<'a, str>>) -> Self {
        self.attributes_mut().set("style", value.into());
        self
    }

    /// Returns the attributes of the element
    fn attributes_mut(&mut self) -> &mut Attributes<'a>;
}

/// A complete HTML document, including the doctype
pub(crate) struct Document<'a>(Html<'a>);

impl<'a> Document<'a> {
    /// Creates a document with the given root element
    pub(crate) fn new(html: Html<'a>) -> Self {
        Self(html)
    }
}

impl Encode for Document<'_> {
    fn encode(&self, out: &mut String) {
        out.push_str("<!DOCTYPE html>");
        self.0.encode(out);
    }
}

/// Text content, escaped when rendered
pub(crate) struct Text<'a>(Cow<'a, str>);

impl<'a> Text<'a> {
    /// Creates a text node
    pub(crate) fn new(text: impl Into<Cow<'a, str>>) -> Self {
        Self(text.into())
    }
}

impl Encode for Text<'_> {
    fn encode(&self, out: &mut String) {
        escape(&self.0, out);
    }
}

impl<'a> From<&'a str> for Text<'a> {
    fn from(text: &'a str) -> Self {
        Self::new(text)
    }
}

impl<'a> From<&'a String> for Text<'a> {
    fn from(text: &'a String) -> Self {
        Self::new(text)
    }
}

impl From<String> for Text<'_> {
    fn from(text: String) -> Self {
        Self::new(text)
    }
}

impl<'a> From<Cow<'a, str>> for Text<'a> {
    fn from(text: Cow<'a, str>) -> Self {
        Self::new(text)
    }
}

/// Markup that is rendered without escaping
///
/// Only use this for markup that is known to be safe, like the output of an HTML sanitizer.
pub(crate) struct Unescaped<'a>(Cow<'a, str>);

impl<'a> Unescaped<'a> {
    /// Wraps markup that is known to be safe
    pub(crate) fn new(html: impl Into<Cow<'a, str>>) -> Self {
        Self(html.into())
    }
}

impl Encode for Unescaped<'_> {
    fn encode(&self, out: &mut String) {
        out.push_str(&self.0);
    }
}

/// The attributes of an element, in the order they were first set
#[derive(Default)]
pub(crate) struct Attributes<'a>(Vec<(&'static str, Option<Cow<'a, str>>)>);

impl<'a> Attributes<'a> {
    fn add_class(&mut self, class: Cow<'a, str>) {
        for (name, value) in &mut self.0 {
            if *name != "class" {
                continue;
            }

            let Some(existing) = value else {
                *value = Some(class);
                return;
            };

            let existing = existing.to_mut();
            existing.push(' ');
            existing.push_str(&class);
            return;
        }

        self.0.push(("class", Some(class)));
    }

    fn set(&mut self, name: &'static str, value: Cow<'a, str>) {
        self.insert(name, Some(value));
    }

    fn set_flag(&mut self, name: &'static str) {
        self.insert(name, None);
    }

    fn insert(&mut self, name: &'static str, value: Option<Cow<'a, str>>) {
        for (existing, slot) in &mut self.0 {
            if *existing == name {
                *slot = value;
                return;
            }
        }

        self.0.push((name, value));
    }
}

impl Encode for Attributes<'_> {
    fn encode(&self, out: &mut String) {
        for (name, value) in &self.0 {
            out.push(' ');
            out.push_str(name);
            let Some(value) = value else { continue };
            out.push_str("=\"");
            escape(value, out);
            out.push('"');
        }
    }
}

fn escape(text: &str, out: &mut String) {
    let mut start = 0;
    for (index, c) in text.char_indices() {
        let escaped = match c {
            '&' => "&amp;",
            '<' => "&lt;",
            '>' => "&gt;",
            '"' => "&quot;",
            '\'' => "&#39;",
            _ => continue,
        };

        out.push_str(&text[start..index]);
        out.push_str(escaped);
        start = index + 1;
    }

    out.push_str(&text[start..]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_text_and_attributes() {
        let html = A::new()
            .href("/search?q=a&b")
            .title("\"quoted\"")
            .child("<script>alert('hi')</script>")
            .to_string();
        assert_eq!(
            html,
            "<a href=\"/search?q=a&amp;b\" title=\"&quot;quoted&quot;\">\
             &lt;script&gt;alert(&#39;hi&#39;)&lt;/script&gt;</a>"
        );
    }

    #[test]
    fn renders_unescaped_markup_verbatim() {
        let html = Div::new()
            .child(Unescaped::new("<b>bold</b>".to_owned()))
            .to_string();
        assert_eq!(html, "<div><b>bold</b></div>");
    }

    #[test]
    fn accumulates_classes() {
        let html = Li::new().class("thread").class("unread").to_string();
        assert_eq!(html, "<li class=\"thread unread\"></li>");
    }

    #[test]
    fn renders_fields_before_children() {
        let html = Details::new(Summary::new().child("Subject"))
            .open()
            .child(P::new().child("Body"))
            .to_string();
        assert_eq!(
            html,
            "<details open><summary>Subject</summary><p>Body</p></details>"
        );
    }

    #[test]
    fn renders_void_elements() {
        let html = Head::new(Title::new("Inbox"))
            .child(Meta::charset("utf-8"))
            .child(Link::new("stylesheet", "/style.css"))
            .to_string();
        assert_eq!(
            html,
            "<head><meta charset=\"utf-8\"><link rel=\"stylesheet\" href=\"/style.css\">\
             <title>Inbox</title></head>"
        );
    }

    #[test]
    fn renders_doctype() {
        let html = Html::new(Head::new(Title::new("Inbox")), Body::new()).lang("en");
        assert_eq!(
            Document::new(html).to_string(),
            "<!DOCTYPE html><html lang=\"en\"><head><title>Inbox</title></head><body></body></html>"
        );
    }
}
