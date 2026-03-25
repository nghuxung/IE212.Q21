data = LOAD '../Output/Bai1/bai1_output'
USING PigStorage('\t')
AS (
    id:chararray,
    review:chararray,
    aspect:chararray,
    category:chararray,
    sentiment:chararray
);

-- Chuẩn hoá sentiment
data_clean = FOREACH data GENERATE
    id,
    review,
    aspect,
    category,
    TRIM(LOWER(sentiment)) AS sentiment;

-- Lọc positive
pos = FILTER data_clean BY sentiment == 'positive';

-- Group theo aspect
grp = GROUP pos BY aspect;

-- Đếm
count_pos = FOREACH grp GENERATE
    group AS aspect,
    COUNT(pos) AS total_positive;

-- Lưu (KHÔNG ORDER để tránh crash)
STORE count_pos INTO '../Output/Bai3_positive_count'
USING PigStorage('\t');