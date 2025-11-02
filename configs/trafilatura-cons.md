**Trafilatura is powerful but has real limitations.** Here's what it can and can't do:

---

## **What Trafilatura DOES well:**

✅ **Extracts main article text** from blogs, news sites, standard articles  
✅ **Removes boilerplate** (nav, ads, footers, sidebars)  
✅ **Works on most WordPress/Medium/Blogspot sites**  
✅ **Preserves structure** (paragraphs, lists, links if you want)  
✅ **Fast and lightweight**  
✅ **Handles varied HTML structures** without custom rules

---

## **Limitations & Weaknesses:**

### **1. Doesn't extract structured metadata well**
❌ **Can't reliably extract:**
- Restaurant names (as distinct from article text)
- Addresses (as structured fields)
- Phone numbers (as structured fields)
- Prices, hours, ratings
- Author, publish date (inconsistent)

**Example:**
```
Trafilatura output:
"Mama Cass is located at 123 Ikeja Road. Call 08012345678 for reservations."
```

You still need **NLP/regex** to extract:
- Name: "Mama Cass"
- Address: "123 Ikeja Road"
- Phone: "08012345678"

**So:** Trafilatura gives you clean text, but you still need your `nlp/fields.py` extraction logic.

---

### **2. Struggles with non-article pages**

❌ **Fails on:**
- **Directory/listing pages** (ofadaa.com restaurant cards)
- **Search result pages** (foodieinlagos.com/?s=amala)
- **Landing pages** with minimal text
- **Menu pages** with tables/structured data
- **Social media posts** (Facebook, Instagram embeds)

**Example:**
A directory page like ofadaa.com with 20 restaurant cards will get mangled:
```
Output: "Restaurant 1 Lagos Restaurant 2 Ibadan Restaurant 3 Ikeja..."
```
All the structure is lost - names, addresses mixed together.

**Solution:** Need custom selectors for directory sites.

---

### **3. Can miss embedded structured data**

❌ **Doesn't extract:**
- **JSON-LD** (schema.org Restaurant/LocalBusiness)
- **Open Graph** metadata
- **Microdata**
- **Google Maps embeds** (iframe links)

**Example:**
A blog post has:
```html
<script type="application/ld+json">
{
  "@type": "Restaurant",
  "name": "Mama Cass",
  "address": "123 Ikeja Road",
  "telephone": "08012345678"
}
</script>
```

Trafilatura **ignores this** - you need BeautifulSoup/lxml to extract it separately.

**Solution:** You already handle this in `extractor/article.py` with `extract_jsonld()`.

---

### **4. Inconsistent with tables/lists**

⚠️ **Tables get flattened:**
```html
<table>
  <tr><td>Amala</td><td>₦500</td></tr>
  <tr><td>Ewedu</td><td>₦200</td></tr>
</table>
```

Trafilatura output:
```
"Amala ₦500 Ewedu ₦200"
```

The row/column structure is lost.

**Solution:** If you need table data (menus, price lists), parse tables separately with CSS selectors.

---

### **5. JavaScript-rendered content**

❌ **Can't handle:**
- Sites that load content via JavaScript (React, Vue, Angular SPAs)
- Infinite scroll
- Dynamic content loading

**Example:**
If a restaurant site uses React and loads data via API calls, trafilatura sees **empty HTML**.

**Solution:** Use **Scrapy-Playwright** or **Selenium** to render JavaScript first, then pass to trafilatura.

---

### **6. Paywalls & login-required content**

❌ **Blocked by:**
- Paywalls
- Members-only content
- Anti-bot protections (Cloudflare, etc.)

Trafilatura doesn't bypass these - it just gets the blocked HTML.

**Solution:** Same as any scraper - handle auth, use proxies, etc.

---

### **7. Multilingual content (sometimes)**

⚠️ Works but not optimized for:
- Non-Latin scripts (Arabic, Chinese) - works but may have encoding issues
- Mixed-language pages - may extract everything or miss context

For your Nigerian use case (English + some Yoruba), it should be fine.

---

### **8. Image/video content**

❌ **Doesn't extract:**
- Image descriptions beyond alt text
- Video transcripts (unless in HTML)
- Captions, embedded content

**Solution:** Need separate extraction for images/videos.

---

## **When Trafilatura is NOT enough:**

| **Page Type** | **Trafilatura Good?** | **What to Use Instead** |
|---------------|----------------------|-------------------------|
| Blog article | ✅ Yes | Trafilatura alone |
| News article | ✅ Yes | Trafilatura alone |
| Directory listing (ofadaa) | ❌ No | Custom CSS/XPath selectors |
| Search results page | ❌ No | Custom selectors |
| Menu page (tables) | ⚠️ Maybe | Custom table parsing |
| Social media post | ❌ No | Platform APIs or custom parsing |
| JavaScript SPA | ❌ No | Scrapy-Playwright |
| Structured JSON-LD | ❌ No | BeautifulSoup (you already do this) |

---

## **For your use case (Amala Atlas):**

### **What Trafilatura handles well:**
✅ Blog reviews (foodieinlagos.com articles)  
✅ City guide articles (eatdrinklagos.com)  
✅ Food blog posts (blog.fusion.ng)

### **What needs custom extraction:**
❌ Directory pages (ofadaa.com restaurant cards)  
❌ JSON-LD data (you already handle this)  
❌ Google Maps links (extract separately from HTML)  
❌ Structured menu/price tables  
❌ Social media embeds

---

## **Recommended Hybrid Approach:**

```python
def parse(self, response):
    # 1. Extract JSON-LD first (structured data)
    jsonld = extract_jsonld(response.text)  # Your existing code
    
    # 2. Extract main text with trafilatura
    import trafilatura
    main_text = trafilatura.extract(response.text)
    
    # 3. Extract additional structured elements
    maps_links = response.css('a[href*="maps.google"]::attr(href)').getall()
    phone_links = response.css('a[href^="tel:"]::attr(href)').getall()
    
    # 4. Run NLP on trafilatura text
    doc = self.nlp(main_text or '')
    entities = extract_entities(doc)  # NLP logic
    
    # 5. Combine all sources
    return {
        'structured_data': jsonld,  # Best source
        'text_content': main_text,   # For NLP
        'entities': entities,        # From NLP
        'maps_links': maps_links,    # Direct extraction
        'phones': phone_links,       # Direct extraction
    }
```

---

## **Alternatives to Trafilatura:**

| **Tool** | **Pros** | **Cons** |
|----------|----------|----------|
| **Trafilatura** | Fast, works on most articles, simple | No structured data, fails on non-articles |
| **Newspaper3k** | Similar to trafilatura, extracts authors/dates better | Slower, sometimes worse at cleaning |
| **Readability** (via readability-lxml) | Good at finding main content | Python port is unmaintained |
| **BeautifulSoup + heuristics** | Full control | Need to write extraction logic yourself |
| **Boilerpipe** (Java) | Very good at content extraction | Requires Java, harder to integrate |
| **Diffbot** (paid API) | Best-in-class extraction | Expensive, external dependency |

**For your project:** Trafilatura + custom extractors is the sweet spot.

---

## **Bottom Line:**

**Trafilatura is great for 70-80% of your sources (blog articles).**

For the other 20-30%:
- Use **custom selectors** for directories (ofadaa.com)
- Use **your existing extractors** for JSON-LD, maps links, phones
- Use **Scrapy-Playwright** if you hit JavaScript-heavy sites

**Your strategy:**
1. ✅ Use trafilatura for **article text extraction**
2. ✅ Keep your **custom extractors** for structured data (JSON-LD, maps, phones)
3. ✅ Write **site-specific spiders** for directories if needed

This gives you the best of both worlds: generic + precise.

---

**Does this help clarify when to use trafilatura vs custom extraction?**