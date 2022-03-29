# ETL (Extract Transform Load)

Before running this application, make sure you have the following:

1. The dataset and popular hashtags under resources directory

```bash
.
├── ...
├── resources                    
│   ├── query2_ref.txt               # raw data
│   ├── popular_hashtags.txt         # popular hashtags
└── ...
```

2. Create a database and tables on your postgres instance

- To create a database, run
```sql
CREATE DATABASE db_name;
```

- Create these tables

```sql
CREATE TABLE public.user_account (
    user_id bigint NOT NULL,
    screen_name character varying,
    description character varying,
    name character varying,
    created_at timestamp without time zone
);
```

```sql
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
```

```sql
CREATE TABLE public.hashtag (
    id serial NOT NULL PRIMARY KEY,
    tweet_id bigint NOT NULL,
    user_id bigint NOT NULL,
    hashtag_name character varying NOT NULL
);
```

Lastly, change to your postgres credentials in `PostgresImport.java` in `connectToDB` method.

Go ahead and run the application 😄