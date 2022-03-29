CREATE TABLE public.tweet (
    tweet_id bigint NOT NULL,
    created_at timestamp without time zone NOT NULL,
    text character varying NOT NULL,
    user_id bigint NOT NULL,
    user_screen_name character varying,
    user_description character varying,
    tweet_type character varying,
    reply_to_tweet_id bigint,
    reply_to_user_id bigint,
    retweet_to_tweet_id bigint,
    retweet_to_user_id bigint,
    hashtags character varying
);