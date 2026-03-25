DEFINE BagToString org.apache.pig.builtin.BagToString();

%default INPUT '../Input/hotel-review.csv'
%default STOPWORDS '../Input/stopwords.txt'
%default OUTPUT '../Output/bai1_output'

raw_data = LOAD '$INPUT'
USING PigStorage(';')
AS (
    id:chararray,
    review:chararray,
    aspect:chararray,
    category:chararray,
    sentiment:chararray
);

data_no_header = FILTER raw_data BY
    id IS NOT NULL AND
    TRIM(id) != '' AND
    id != 'id' AND
    review IS NOT NULL AND
    TRIM(review) != '';

raw_stop = LOAD '$STOPWORDS'
USING PigStorage('\n')
AS (word:chararray);

stopwords = FILTER raw_stop BY
    word IS NOT NULL AND
    TRIM(word) != '';

stopwords_clean = FOREACH stopwords GENERATE
    LOWER(TRIM(word)) AS word;

stopwords_distinct = DISTINCT stopwords_clean;

normalized = FOREACH data_no_header GENERATE
    TRIM(id) AS id,
    REPLACE(LOWER(review), '[.,!?;:()\\[\\]{}/"-]', ' ') AS review_clean,
    TRIM(aspect) AS aspect,
    TRIM(category) AS category,
    TRIM(sentiment) AS sentiment;

token_bag = FOREACH normalized GENERATE
    id,
    aspect,
    category,
    sentiment,
    TOKENIZE(review_clean) AS tokens;

token_rows = FOREACH token_bag GENERATE
    id,
    aspect,
    category,
    sentiment,
    FLATTEN(tokens) AS token;

tokens_valid = FILTER token_rows BY
    token IS NOT NULL AND
    SIZE(token) > 1;

ranked_tokens = RANK tokens_valid BY id, aspect, category, sentiment;

joined = JOIN ranked_tokens BY token LEFT OUTER, stopwords_distinct BY word;

filtered = FILTER joined BY stopwords_distinct::word IS NULL;

selected_tokens = FOREACH filtered GENERATE
    ranked_tokens::id AS id,
    ranked_tokens::aspect AS aspect,
    ranked_tokens::category AS category,
    ranked_tokens::sentiment AS sentiment,
    ranked_tokens::token AS token,
    ranked_tokens::rank_tokens_valid AS pos;

grp = GROUP selected_tokens BY (id, aspect, category, sentiment);

result = FOREACH grp {
    ordered = ORDER selected_tokens BY pos;
    words = FOREACH ordered GENERATE token;

    GENERATE
        group.id AS id,
        BagToString(words, ' ') AS cleaned_review,
        group.aspect AS aspect,
        group.category AS category,
        group.sentiment AS sentiment;
};

STORE result INTO '$OUTPUT' USING PigStorage('\t');