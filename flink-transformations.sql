CREATE
MODEL sentiment_analysis_model
INPUT(text STRING)
OUTPUT(sentiment_analysis_response STRING)
COMMENT 'chatbot'
WITH (
  'provider' = 'openai',
  'task' = 'text_generation',
  'openai.connection' = 'your-openai.connection-create-with-confluent-cli',
  'openai.model_version' = 'gpt-3.5-turbo',
  'openai.system_prompt' =
  'You are a sentiment analysis specialist. You will be given a piece of text and you need to analyze the sentiment of the text. Give your response as one word: POSITIVE, NEGATIVE or NEUTRAL, no other text or punctuation.');


CREATE
MODEL category_analysis_model
INPUT(text STRING)
OUTPUT(category_analysis_response STRING)
COMMENT 'chatbot based on openai gpt 3.5 turbo'
WITH (
  'provider' = 'openai',
  'task' = 'text_generation',
  'openai.connection' = 'your-openai.connection-create-with-confluent-cli',
  'openai.model_version' = 'gpt-3.5-turbo',
  'openai.system_prompt' =
  'You are a category analysis specialist. You will be given a piece of text and you need to match the closest category to the text. Give your response as one of the following categories: Personal Updates, News, Entertainment, Humor, Inspirational, Food, Travel, Health, Technology, Business, Sports, Art, Fashion, Animals, Cars, Music, Gaming, Social Issues, Educational, Programming, Science, Politics, Other. Return only the category name, no other text or punctuation.');



CREATE TABLE hashtags (
    tag    STRING NOT NULL,
    tag_count     INT NOT NULL
)



CREATE TABLE messages_with_ml_analysis (
    author                         STRING NOT NULL,
    text                           STRING NOT NULL,
    postedAt                       STRING NOT NULL,
    images                         INT NOT NULL,
    hashtags                       ARRAY<STRING> NOT NULL,
    mentions                       ARRAY<STRING> NOT NULL,
    linkCount                      INT NOT NULL,
    repostBy                       STRING,
    replyTo                        STRING,
    likeCount                      INT NOT NULL,
    repostCount                    INT NOT NULL,
    replyCount                     INT NOT NULL,
    quoteCount                     INT NOT NULL,
    lang                           STRING,
    sentiment_analysis_response    STRING,
    category_analysis_response     STRING
) 



INSERT INTO messages_with_ml_analysis
SELECT
    b.author,
    b.text,
    b.postedAt,
    b.images,
    b.hashtags,
    b.mentions,
    b.linkCount,
    b.repostBy,
    b.replyTo,
    b.likeCount,
    b.repostCount,
    b.replyCount,
    b.quoteCount,
    b.lang,
    sa.sentiment_analysis_response,
    ca.category_analysis_response
FROM BlueSkyMessages AS b WHERE text IS NOT NULL AND text <> '',
LATERAL TABLE(
    ML_PREDICT(
        'sentiment_analysis_model',
        CONCAT(
            'This is information from the bluesky post ', b.text,
            ' Tell me the sentiment of the post: POSITIVE, NEGATIVE or NEUTRAL.'
        )
    )
) AS sa,
LATERAL TABLE(
    ML_PREDICT(
        'category_analysis_model',
        CONCAT(
            'This is information from the bluesky post ', b.text, 
            ' Tell me the category of the post: Personal Updates, News and Current Events, Entertainment, Humor and Memes, Inspirational and Motivational, Food and Culinary, Travel and Adventure, Health and Fitness, Technology, Business and Entrepreneurship, Sports, Art and Photography, Fashion and Beauty, Pets and Animals, Cars and Automobiles, DIY and Home Decor, Music, Gaming, Environmental and Social Issues, Educational and Informative, Programming, Science, Politics, Other. Return only the category name.'
        )
    )
) AS ca;

---

INSERT INTO message_analysis_output
SELECT
    b.author,
    b.text,
    b.postedAt,
    b.images,
    b.hashtags,
    b.mentions,
    b.linkCount,
    b.repostBy,
    b.replyTo,
    b.likeCount,
    b.repostCount,
    b.replyCount,
    b.quoteCount,
    b.lang,
    sa.sentiment_analysis_response,
    ca.category_analysis_response
FROM messages_no_duplicates AS b,
LATERAL TABLE(
    ML_PREDICT(
        'sentiment_analysis_model',
        CONCAT(
            'This is information from the bluesky post ', b.text,
            ' Tell me the sentiment of the post: POSITIVE, NEGATIVE or NEUTRAL.'
        )
    )
) AS sa,
LATERAL TABLE(
    ML_PREDICT(
        'category_analysis_model',
        CONCAT(
            'This is information from the bluesky post ', b.text, 
            ' Tell me the category of the post: Personal Updates, News and Current Events, Entertainment, Humor and Memes, Inspirational and Motivational, Food and Culinary, Travel and Adventure, Health and Fitness, Technology, Business and Entrepreneurship, Sports, Art and Photography, Fashion and Beauty, Pets and Animals, Cars and Automobiles, DIY and Home Decor, Music, Gaming, Environmental and Social Issues, Educational and Informative, Programming, Science, Politics, Other. Return only the category name.'
        )
    )
) AS ca;


CREATE TABLE post_analysis_output (
    author                         STRING NOT NULL,
    text                           STRING NOT NULL,
    postedAt                       STRING NOT NULL,
    images                         INT NOT NULL,
    hashtags                       ARRAY<STRING> NOT NULL,
    mentions                       ARRAY<STRING> NOT NULL,
    linkCount                      INT NOT NULL,
    repostBy                       STRING,
    replyTo                        STRING,
    likeCount                      INT NOT NULL,
    repostCount                    INT NOT NULL,
    replyCount                     INT NOT NULL,
    quoteCount                     INT NOT NULL,
    lang                           STRING,
    sentiment_analysis_response    STRING,
    category_analysis_response     STRING
);

INSERT INTO post_analysis_output
SELECT
    author,
    text,
    postedAt,
    images,
    hashtags,
    mentions,
    linkCount,
    repostBy,
    replyTo,
    likeCount,
    repostCount,
    replyCount,
    quoteCount,
    lang,
    sentiment_analysis_response,
    category_analysis_response
FROM (
    SELECT
        b.author,
        b.text,
        b.postedAt,
        b.images,
        b.hashtags,
        b.mentions,
        b.linkCount,
        b.repostBy,
        b.replyTo,
        b.likeCount,
        b.repostCount,
        b.replyCount,
        b.quoteCount,
        b.lang,
        sa.sentiment_analysis_response,
        ca.category_analysis_response,
        ROW_NUMBER() OVER (PARTITION BY b.text ORDER BY b.postedAt DESC) AS rn
    FROM BlueSkyMessages AS b
    LATERAL TABLE(
        ML_PREDICT(
            'sentiment_analysis_model',
            CONCAT(
                'This is information from the bluesky post ', b.text,
                '--- Tell me the sentiment of the post: POSITIVE, NEGATIVE or NEUTRAL. Return only the sentiment, no other text or punctuation.'
            )
        )
    ) AS sa
    LATERAL TABLE(
        ML_PREDICT(
            'category_analysis_model',
            CONCAT(
                'This is information from the bluesky post ', b.text, 
                '--- Tell me the category of this post, here are the possible categories: Personal Updates, News, Entertainment, Humor, Inspirational, Food, Travel, Health, Technology, Business, Sports, Art, Fashion, Animals, Cars, Music, Gaming, Social Issues, Educational, Programming, Science, Politics, Other. Return only the category name, no other text or punctuation.'
            )
        )
    ) AS ca
) t
WHERE rn = 1;


