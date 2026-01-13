# Web_scraping-GDELT--EDA
A Collection of codes used for Scraping Stock news using GDELT API and EDA on the scraped text

📌 Data Collection & Exploratory Data Analysis (EDA)
1. GDELT API – Global News Web Scraping

This project uses the GDELT (Global Database of Events, Language, and Tone) API as the primary data source for collecting global stock-market–related news.

GDELT is an open, publicly available news intelligence platform that monitors broadcast, print, and online news media across the world in near real time. Using its Events and Mentions datasets, this project programmatically scraped news articles related to 96 publicly listed companies across global markets, ensuring broad geographic and sectoral coverage.

Key aspects of data collection:

API-based web scraping using keyword-driven company identifiers

Time-bounded queries to maintain API stability and compliance

News coverage includes article metadata, source, timestamps, and textual content

Focused specifically on stock-related and corporate news

2. Exploratory Data Analysis (EDA) Framework

After data ingestion and preprocessing, a comprehensive EDA pipeline was built using Sweetviz and Plotly, enabling both automated statistical profiling and interactive visual exploration.

Tools & Libraries Used

Sweetviz – automated EDA and data quality profiling

Plotly – interactive, filter-driven visual analytics

Kaleido – static image export compatibility for Plotly (PNG/HTML)

Pandas / NumPy – data wrangling and transformations

3. Interactive EDA Design & Cascading Filters

The EDA dashboard is designed with cascading filters, allowing users to drill down into the data dynamically.
Filter selections automatically update all dependent visuals and text analytics.

Primary cascading filters include:

Company Name

Country / Region

News Source

Date / Month (derived Month-Year field)

News Category / Theme

This enables:

Company-specific news trend analysis

Regional sentiment and coverage comparison

Source-level news bias and frequency exploration

Time-series inspection of news volume and patterns

4. TF-IDF–Based Keyword Importance Analysis

To extract meaningful insights from textual news data, a TF-IDF (Term Frequency–Inverse Document Frequency) approach was applied.

Highlights of the text analytics layer:

Tokenization and cleaning of news titles and article bodies

TF-IDF vectorization to identify context-specific important keywords

Blended TF-IDF scores computed per filter selection

Dynamic display of top-weighted keywords for:

Selected company

Selected country

Selected time window

Selected news source

This helps surface:

Dominant narratives influencing stock-related news

Company-specific thematic signals

Shifts in media focus across time and geography

5. Output Formats & Visualization Export

The EDA outputs are generated in multiple formats to support different consumption needs:

Interactive HTML dashboards (Plotly)

Automated EDA HTML reports (Sweetviz)

Static image exports (PNG) using Kaleido

Compatible with:

GitHub preview

Academic submissions

Presentations and reports

6. Limitations & Data Constraints

While GDELT is a powerful global news intelligence platform, it has inherent limitations due to its free and open-access nature:

⚠️ Historical depth limitation:
Reliable and consistent scraping is practically limited to ~3 months of recent data to stay within safe usage limits.

⚠️ API throttling and stability constraints:
High-frequency or long-horizon queries may lead to incomplete data retrieval.

⚠️ Article availability variance:
Coverage density varies by region, language, and media source.

⚠️ No guaranteed completeness:
GDELT aggregates publicly available news and does not ensure full coverage of all corporate events.

Despite these constraints, the dataset is well-suited for:

Short-term market sentiment analysis

News-driven exploratory research

Event-based stock movement studies

Proof-of-concept NLP and time-series modeling

7. Summary

This pipeline demonstrates a scalable, reproducible workflow for:

Global stock-news data scraping using GDELT

Automated and interactive EDA using Sweetviz and Plotly

Keyword-driven insight extraction using TF-IDF

Multi-company, multi-region exploratory analysis within real-world API constraints
