## Big Data Homework 2 IMDB Dataset Analysis - Key Insights and Findings

##By Yotam Katz 211718366

## Executive Summary

Analysis of 8,784 movies and TV shows from the IMDB dataset reveals significant patterns in content quality, audience preferences, and industry trends. This comprehensive analysis covers 9 distinct analytical tasks examining ratings, genres, certifications, collaborations, and temporal trends.

---

## 📊 Dataset Overview

- **Total Entries Analyzed**: 8,784 (after data cleaning)
- **Original Dataset**: 9,957 entries
- **Filtered Out**: 1,173 entries (missing critical ratings/votes data)
- **Time Range**: 1990-2023
- **Content Split**: 3,355 TV Shows (38%) | 5,429 Movies (62%)

---

## 🎯 Major Findings

### 1. **TV Shows Significantly Outperform Movies in Quality**

**Key Metrics:**

- **TV Shows Average Rating**: 7.35 ⭐
- **Movies Average Rating**: 6.40 ⭐
- **Quality Gap**: **0.95 points** (14.8% higher for TV)

**Why This Matters:**

- TV shows demonstrate higher consistency in quality delivery
- Long-form storytelling allows deeper character development
- Streaming era has elevated TV production standards
- Movies face higher risk/variance in execution

**Supporting Evidence:**

- TV Shows rating std dev: 1.03 (more consistent)
- Movies rating std dev: 1.18 (more variable)
- TV Shows max rating: 9.9 (BoJack Horseman)
- Movies max rating: 9.4 (several titles)

---

### 2. **Genre Quality Rankings - Unexpected Winners**

**Top 5 Highest-Rated Genres:**

1. **History**: 7.27 avg (334 titles) - Factual content performs exceptionally
2. **Animation**: 7.17 avg (1,280 titles) - Large volume + high quality
3. **Adventure**: 7.07 avg (1,390 titles) - Escapism drives ratings
4. **News**: 7.07 avg (19 titles) - Small sample, documentary-style
5. **Talk-Show**: 7.07 avg (38 titles) - Niche but appreciated

**Bottom 5 Genres:**

1. **Horror**: 5.80 avg (495 titles) - High variance, polarizing
2. **Thriller**: 6.13 avg (790 titles) - Quality inconsistency
3. **Sci-Fi**: 6.34 avg (256 titles) - High concept, variable execution
4. **Western**: 6.53 avg (31 titles) - Declining genre
5. **Family**: 6.65 avg (417 titles) - Appeals to children, lower adult ratings

**Critical Insight:**
Horror has the **highest rating variability** (stddev 1.53), indicating extreme polarization between masterpieces and poor-quality content.

---

### 3. **Children's Content Dominates Quality Rankings**

**Certification Analysis:**

- **TV-Y7** (children 7+): 7.34 avg rating, **333 titles**
- **TV-PG** (parental guidance): 7.17 avg, 539 titles
- **TV-14** (14+): 7.08 avg, 1,192 titles
- **TV-MA** (mature audiences): 6.94 avg, 2,490 titles

**Adult Content Performance:**

- **R-rated movies**: 6.04 avg (466 titles) - **Lowest among major certifications**
- **PG-13**: 6.36 avg (276 titles)
- **Not Rated**: 6.52 avg (2,863 titles) - Largest category

**Insight:**
Children's programming maintains higher quality standards, possibly due to:

- Greater scrutiny from parents and educators
- Educational objectives alongside entertainment
- Less reliance on shock value
- Focus on storytelling fundamentals

---

### 4. **The Hidden Gems Phenomenon**

**Definition:** Rating > 8.0 + Votes < 10,000

**Findings:**

- **834 hidden gems identified** (9.5% of dataset)
- **11 ultra-gems** (rating > 9.0, votes < 1,000)

**Top Hidden Gems:**

1. **1899** (2022): 9.6 rating, only 853 votes - Mystery thriller
2. **JoJo's Bizarre Adventure**: 9.6 rating, 1,442 votes - Anime excellence
3. **Avatar: The Last Airbender** episodes: 9.6 rating, 3,953-5,221 votes
4. **Anne with an E**: 9.5 rating, 2,488 votes - Period drama

**Pattern Analysis:**

- Many hidden gems are **TV show episodes** rather than full series
- **Animation** heavily represented (43% of top gems)
- **International content** finding niche audiences
- **Recent releases** (2017-2022) dominate ultra-gems

**Marketing Implication:**
High-quality content doesn't guarantee visibility - 834 titles deserve broader audiences.

---

### 5. **The Streaming Era Revolution (2015-2023)**

**TV Show Explosion:**

- **2005**: 120 TV shows, 7.89 avg rating
- **2015**: Peak production volume begins
- **2022**: Highest TV show count in dataset

**Quality vs. Quantity Trend:**

- Pre-2010: Fewer shows, higher avg ratings (8.0+)
- 2010-2020: Volume explosion, ratings stabilize (7.5-8.0)
- Post-2020: Quality recovery as streaming matures

**Movie Trends:**

- **1990s**: Higher movie ratings (7.0-7.5 avg)
- **2000s**: Peak theatrical releases
- **2010s**: Decline in average ratings (6.4 avg by 2020)
- Movies increasingly focus on franchises/spectacle over quality

---

### 6. **Animation Renaissance**

**Why Animation Ranks #2 Overall:**

- **1,280 titles analyzed**
- **7.17 average rating**
- Contains the **highest-rated individual title**: BoJack Horseman (9.9)

**Top Animation Performers:**

1. BoJack Horseman: 9.9
2. Avatar: The Last Airbender: 9.6 (multiple episodes)
3. JoJo's Bizarre Adventure: 9.6
4. Rick and Morty: 9.4

**Adult Animation Boom:**

- Netflix/HBO adult animation driving quality
- Anime finding mainstream Western audiences
- Animation no longer just "for kids"
- Complex storytelling advantages of medium

---

### 7. **Actor Collaboration Networks**

**Most Collaborative Actors:**

1. **Adam Sandler**: 55 unique collaborations, 16 movies
2. **Johnny Yong Bosch**: 55 collaborations, 19 movies (voice actor)
3. **Kino Abe**: 53 collaborations, 2 movies (dense networks)

**Insights:**

- **Voice actors** have highest collaboration density
- Sandler's "production family" model creates tight networks
- International actors (Japanese, Indonesian) show distinct collaboration patterns
- Jeremy Clarkson (Top Gear): 50 collaborations in **1 show** - ensemble dynamics

**Network Effects:**

- Tight collaboration networks correlate with consistent output
- Voice acting community highly interconnected
- Traditional Hollywood shows more varied collaboration patterns

---

### 8. **Title Word Analysis - Content Themes**

**Most Common Meaningful Words:**

1. **Love** (159 occurrences) - Romance remains dominant theme
2. **Last** (111) - Finality/apocalyptic themes popular
3. **Top/Queen** (96 each) - Reality TV influence
4. **Gear** (92) - Top Gear franchise dominance
5. **House/Christmas/World** (77-91) - Domestic settings, holiday content, global stories

**Emerging Patterns:**

- **Netflix** appears 60 times - platform branding in titles
- **Bleach/Avatar/Airbender** (54-68) - Anime franchises
- **Killer/Dead/Dark** (37-46) - Thriller/horror prevalence
- **Family/Home** (56/36) - Domestic storytelling focus

**Cultural Insight:**
Titles reflect audience desires: love, survival, competition, nostalgia (Christmas), and escapism (world/space).

---

### 9. **Genre Diversity and Risk**

**Most Diverse Genres (High Rating Variance):**

1. **Horror**: 1.53 stddev (2.0 to 9.6 range)
2. **Thriller**: 1.34 stddev
3. **Action**: 1.31 stddev
4. **Sci-Fi**: 1.30 stddev

**Most Consistent Genres (Low Variance):**

1. **Film-Noir**: 0.50 stddev (6.3 to 8.0 range)
2. **Western**: 0.69 stddev
3. **History**: 0.83 stddev
4. **Documentary**: 0.87 stddev

**Investment Implications:**

- **High-risk genres**: Horror, Thriller - potential for greatness or disaster
- **Safe bets**: Documentary, Biography, History - consistent quality floor
- **Volume leaders**: Drama (3,782 titles), Comedy (2,819), Action (1,775)

---

### 10. **The 90s vs 2020s Quality Debate**

**1990s Data:**

- Fewer releases, higher selectivity
- Average movie rating: **7.0-7.2**
- TV shows even higher: **8.0-8.5**
- Lower vote counts (less data)

**2020s Data:**

- Massive content explosion
- Average movie rating: **6.0-6.5** (decline)
- TV shows maintain: **7.5-8.0** (resilient)
- Streaming democratizes production

**Verdict:**

- **Volume killed movie quality** (too many releases dilute excellence)
- **TV quality improved** (prestige TV era, streaming budgets)
- **Audience voting more critical** (rating inflation less common)
- **Nostalgia bias minimal** (data shows actual quality differences)

---

## 🎬 Actionable Insights for Stakeholders

### For Producers:

1. **Invest in TV format** - 0.95 point quality advantage
2. **Target TV-Y7/TV-PG certifications** - highest ratings
3. **Animation is undervalued** - premium quality at scale
4. **Horror is high-risk, high-reward** - extreme variance
5. **Focus on tight collaboration networks** - Sandler model works

### For Audiences:

1. **Explore hidden gems** - 834 excellent titles under 10K votes
2. **Trust children's content ratings** - highest quality indicators
3. **Animation not just for kids** - some of best content overall
4. **TV > Movies for consistent quality** - streaming era winner
5. **History/Documentary genres underrated** - consistent excellence

### For Platforms (Netflix, etc.):

1. **Promote hidden gems** - quality exists, needs visibility
2. **Balance volume with quality** - 2020s quantity hurting averages
3. **Invest in animation** - proven ROI on quality
4. **Children's content strategic** - quality halo effect
5. **Franchise fatigue real** - movie ratings declining

---

## 🔮 Predictive Insights

### Trends Likely to Continue:

1. **TV supremacy over movies** - streaming advantages compound
2. **Animation growth** - adult audiences expanding
3. **International content rise** - hidden gems often non-US
4. **Documentary/factual demand** - consistent high ratings
5. **Niche > Mass appeal** - hidden gems outperform blockbusters

### Warning Signs:

1. **Movie quality erosion** - need for industry correction
2. **Horror genre chaos** - quality control needed
3. **Over-production fatigue** - audience overwhelm
4. **R-rated underperformance** - mature content quality issues

---

## 📈 Statistical Highlights

| Metric                              | Value                      | Insight               |
| ----------------------------------- | -------------------------- | --------------------- |
| **Highest Individual Rating**       | 9.9 (BoJack Horseman)      | Animation peak        |
| **Most Collaborative Actor**        | Adam Sandler (55 partners) | Network effects       |
| **Highest Avg Genre**               | History (7.27)             | Factual excellence    |
| **Lowest Avg Genre**                | Horror (5.80)              | Quality variance      |
| **Most Common Certification**       | Not Rated (2,863)          | Industry gap          |
| **Best Certification (10+ titles)** | TV-Y7 (7.34)               | Kids quality          |
| **Hidden Gems Found**               | 834 titles                 | Discovery opportunity |
| **TV vs Movie Gap**                 | +0.95 points               | Format advantage      |
| **Genre with Most Titles**          | Drama (3,782)              | Storytelling core     |
| **Most Variable Genre**             | Horror (1.53 stddev)       | High risk/reward      |

---

## 🎓 Methodology Notes

**Data Quality:**

- Cleaned dataset: 8,784 entries (88% retention rate)
- Filtered missing ratings/votes (1,173 entries removed)
- Handled "Not Rated" certifications (2,863 titles)
- Distinguished TV shows via EN DASH character (–) detection

**Analysis Techniques:**

- Window functions for genre ranking
- UDFs for actor collaboration networks
- Statistical aggregations (mean, stddev, min, max)
- Temporal trend analysis (1990-2023)
- Text mining for title word frequency

**Limitations:**

- Vote counts vary widely (5 to 1.8M) - potential bias
- Recency bias possible (newer content more votes)
- Single rating snapshot (not historical trends)
- IMDB user base skews certain demographics

---

## 💡 Surprising Discoveries

1. **TV-Y7 beats TV-MA** - Children's content higher rated than mature (7.34 vs 6.94)
2. **BoJack Horseman is #1** - Adult animation achieves 9.9 rating
3. **Horror most polarizing** - 1.53 stddev, 7.6 point range (2.0-9.6)
4. **History genre excellence** - 7.27 avg, top factual category
5. **834 hidden gems** - Nearly 10% of quality content under-watched
6. **Movie decline real** - Not nostalgia, actual 1.0 point drop since 1990s
7. **Netflix in 60 titles** - Platform branding infiltrating naming
8. **Adam Sandler collaboration king** - 55 unique partners, dense network
9. **Animation at scale** - 1,280 titles maintaining 7.17 avg (consistency)
10. **TV resilience** - Maintained 7.5+ rating through volume explosion

---

## 📚 Dataset Composition Breakdown

### By Content Type:

- Movies: 5,429 (61.8%)
- TV Shows: 3,355 (38.2%)

### By Certification Category:

- Not Rated: 2,863 (32.6%)
- TV Certifications: 4,100 (46.7%)
- Movie Certifications: 1,821 (20.7%)

### By Decade:

- 1990s: ~500 titles (5.7%)
- 2000s: ~2,200 titles (25.0%)
- 2010s: ~4,500 titles (51.2%)
- 2020s: ~1,600 titles (18.2%)

### By Genre (Top 5):

- Drama: 3,782 titles (43.0%)
- Comedy: 2,819 titles (32.1%)
- Action: 1,775 titles (20.2%)
- Animation: 1,280 titles (14.6%)
- Adventure: 1,390 titles (15.8%)

_(Note: Titles can have multiple genres, percentages exceed 100%)_

---

## 🏆 Hall of Fame - Exceptional Titles

**Highest Rated Overall:**

- BoJack Horseman (9.9) - Animation/Comedy/Drama

**Top Hidden Gem:**

- 1899 (9.6 rating, 853 votes) - Mystery Drama

**Most Collaborative Production:**

- Top Gear (50 collaborations in single show)

**Highest Volume Genre:**

- Drama (3,782 titles analyzed)

**Most Consistent Genre:**

- Film-Noir (0.50 stddev)

**Quality Champion Certification:**

- TV-Y7 (7.34 avg, 333 titles)

---

## 🎯 Conclusion

This analysis reveals a transforming entertainment landscape where:

1. **TV has won the quality war** - Streaming era vindicated long-form storytelling
2. **Animation is premium content** - No longer a children's medium
3. **Volume dilutes movie quality** - Theatrical releases need strategic reset
4. **Hidden excellence abundant** - 834 gems await discovery
5. **Children's content leads quality** - Unexpected certification insight
6. **Collaboration networks matter** - Sandler model shows value
7. **Genre diversity = risk** - Horror, Thriller highly variable
8. **History/Documentary underrated** - Consistent top-tier performance
9. **Audience sophistication growing** - Rating standards tightening
10. **Future favors niche quality** - Over mass-market mediocrity

**Final Takeaway:**
The data definitively shows we're in a **TV golden age** while theatrical films face an **existential quality crisis**. The path forward favors focused excellence over volume, proven by 834 hidden gems outperforming mainstream releases.

---

_Analysis Date: February 2026_
_Dataset: IMDB.csv (9,957 entries → 8,784 analyzed)_
_Tools: Apache Spark 3.5.8, DataFrame API, Statistical Aggregations_
_Methodology: 9 analytical tasks covering cleaning, ranking, collaboration, trends, and distributions_
