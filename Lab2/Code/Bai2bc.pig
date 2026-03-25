-- Đọc dữ liệu từ output của Bài 1
bai1_data = LOAD '../Output/Bai1/bai1_output'
USING PigStorage('\t')
AS (
    id:chararray,
    review:chararray,
    aspect:chararray,
    category:chararray,
    sentiment:chararray
);

-- =========================
-- 1. Thống kê tần số từ
-- =========================

words = FOREACH bai1_data GENERATE FLATTEN(TOKENIZE(LOWER(review))) AS word;

words_clean = FILTER words BY
    word IS NOT NULL AND
    SIZE(word) > 2;

grouped_words = GROUP words_clean BY word;

word_counts = FOREACH grouped_words GENERATE
    group AS word,
    COUNT(words_clean) AS freq;

ordered_words = ORDER word_counts BY freq DESC;

top5_words = LIMIT ordered_words 5;

-- =========================
-- 2. Thống kê theo category
-- =========================

grouped_category = GROUP bai1_data BY category;

count_category = FOREACH grouped_category GENERATE
    group AS category,
    COUNT(bai1_data) AS total_comments;

ordered_category = ORDER count_category BY total_comments DESC;

-- =========================
-- 3. Thống kê theo aspect
-- =========================

grouped_aspect = GROUP bai1_data BY aspect;

count_aspect = FOREACH grouped_aspect GENERATE
    group AS aspect,
    COUNT(bai1_data) AS total_comments;

ordered_aspect = ORDER count_aspect BY total_comments DESC;

-- =========================
-- Lưu kết quả
-- =========================

STORE top5_words INTO '../Output/Bai2_top5' USING PigStorage('\t');
STORE ordered_category INTO '../Output/Bai2_category' USING PigStorage('\t');
STORE ordered_aspect INTO '../Output/Bai2_aspect' USING PigStorage('\t');