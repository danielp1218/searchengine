use lol_html::{element, text, HtmlRewriter, Settings};
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Serialize, Deserialize)]
pub struct MetaTag {
    pub name: String,
    pub content: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ParsedHtml {
    pub url: String,
    pub language: Option<String>,
    pub title: Option<String>,
    pub meta_tags: Vec<MetaTag>,
    pub canonical_url: Option<String>,
    pub content_text: String,
    pub links: Vec<String>,
}

pub fn parse_html(input: String, base_url: &str) -> ParsedHtml {
    let base_url_parsed = Url::parse(base_url).expect("Failed to parse base URL");

    let mut links = Vec::new();
    let mut meta_tags = Vec::new();
    let mut title = None;
    let mut language = None;
    let mut canonical_url = None;
    let mut content_text = String::new();

    let mut rewriter = HtmlRewriter::new(
        Settings {
            element_content_handlers: vec![
                // Extract language from html tag
                element!("html[lang]", |el| {
                    if let Some(lang) = el.get_attribute("lang") {
                        language = Some(lang);
                    }
                    Ok(())
                }),
                // Extract title
                text!("title", |t| {
                    if title.is_none() {
                        title = Some(t.as_str().to_string());
                    } else {
                        title = Some(format!("{}{}", title.as_ref().unwrap(), t.as_str()));
                    }
                    Ok(())
                }),
                // Extract meta tags
                element!("meta[name][content]", |el| {
                    if let (Some(name), Some(content)) = (el.get_attribute("name"), el.get_attribute("content")) {
                        meta_tags.push(MetaTag { name, content });
                    }
                    Ok(())
                }),
                // Extract canonical URL
                element!("link[rel=canonical]", |el| {
                    if let Some(href) = el.get_attribute("href") {
                        canonical_url = Some(href);
                    }
                    Ok(())
                }),
                // Extract links
                element!("a[href]", |el| {
                    if let Some(attached_url) = el.get_attribute("href") {
                        if let Ok(attached_url) = Url::parse(&attached_url) {
                            if attached_url.scheme() == "http" || attached_url.scheme() == "https" {
                                links.push(attached_url.to_string());
                                return Ok(());
                            }
                        }
                        if let Ok(joined_url) = base_url_parsed.join(&attached_url) {
                            links.push(joined_url.to_string());
                        }
                    }
                    Ok(())
                }),
                // Extract text content from body
                text!("body *", |t| {
                    if !t.as_str().trim().is_empty() {
                        if !content_text.is_empty() {
                            content_text.push(' ');
                        }
                        content_text.push_str(t.as_str().trim());
                    }
                    Ok(())
                }),
            ],
            ..Settings::new()
        },
        |_: &[u8]| {}
    );

    rewriter.write(input.as_bytes()).expect("Failed to parse HTML input");
    rewriter.end().expect("Failed to complete HTML parsing");

    ParsedHtml {
        url: base_url.to_string(),
        language,
        title,
        meta_tags,
        canonical_url,
        content_text,
        links,
    }
}