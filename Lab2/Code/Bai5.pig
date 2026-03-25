data = LOAD '../Output/Bai1/bai1_output'
USING PigStorage('\t')
AS (
    id:chararray,
    review:chararray,
    aspect:chararray,
    category:chararray,
    sentiment:chararray
);

data_clean = FOREACH data GENERATE
    id,
    review,
    TRIM(category) AS category;

words = FOREACH data_clean GENERATE
    category,
    FLATTEN(TOKENIZE(review)) AS word;

words_clean = FILTER words BY
    word IS NOT NULL AND
    SIZE(word) > 2;

grp_word = GROUP words_clean BY (category, word);

word_count = FOREACH grp_word GENERATE
    group.category AS category,
    group.word AS word,
    COUNT(words_clean) AS freq;

grp_cat = GROUP word_count BY category;

top5_related = FOREACH grp_cat {
    sorted = ORDER word_count BY freq DESC;
    top = LIMIT sorted 5;
    GENERATE FLATTEN(top);
};

STORE top5_related INTO '../Output/Bai5_output'
USING PigStorage('\t');