CREATE TABLE public.hashtag (
    id serial NOT NULL PRIMARY KEY,
    tweet_id bigint NOT NULL,
    user_id bigint NOT NULL,
    hashtag_name character varying NOT NULL
);