bai1_data = LOAD '../Output/Bai1/bai1_output'
USING PigStorage('\t')
AS (
    id:chararray,
    review:chararray,
    aspect:chararray,
    category:chararray,
    sentiment:chararray
);

words = FOREACH bai1_data GENERATE FLATTEN(TOKENIZE(LOWER(review))) AS word;

words_clean = FILTER words BY
    word IS NOT NULL AND
    SIZE(word) >= 2;

grouped_words = GROUP words_clean BY word;

word_counts = FOREACH grouped_words GENERATE
    group AS word,
    COUNT(words_clean) AS freq;

STORE word_counts INTO '../Output/Bai2a_count' USING PigStorage('\t');