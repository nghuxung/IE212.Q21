data = LOAD '../Output/Bai1/bai1_output'
USING PigStorage('\t')
AS (id:chararray, review:chararray, aspect:chararray, category:chararray, sentiment:chararray);

data_clean = FOREACH data GENERATE
    id,
    review,
    aspect,
    category,
    TRIM(LOWER(sentiment)) AS sentiment;

neg = FILTER data_clean BY sentiment == 'negative';

grp = GROUP neg BY aspect;

count_neg = FOREACH grp GENERATE
    group AS aspect,
    COUNT(neg) AS total_negative;

STORE count_neg INTO '../Output/Bai3_negative_count' USING PigStorage('\t');