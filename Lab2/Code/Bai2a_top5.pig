word_count = LOAD '../Output/Bai2a_count'
USING PigStorage('\t')
AS (word:chararray, freq:long);

ordered_words = ORDER word_count BY freq DESC;

top5_words = LIMIT ordered_words 5;

STORE top5_words INTO '../Output/Bai2_top5' USING PigStorage('\t');