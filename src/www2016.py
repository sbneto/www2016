import numpy as np
from utils import graphs
from utils.bigquery import run_query

__author__ = 'Samuel'


def multiplot_cumulative(project, query, axes=('linear', 'linear'), columns_type=(float, str, float),
                         normalize='', loc='upper left', **kwargs):
    data, labels = run_query(project, query, columns_type)
    groups = data[1]
    y = data[2]
    for group in np.nditer(np.unique(groups), ['refs_ok']):
        curr_group = groups == group
        total_group = np.sum(y[curr_group]) if normalize == 'sum' else 1.0
        y[curr_group] = np.cumsum(y[curr_group]) / total_group
    graphs.multiplot(data, labels, axes, normalize=normalize, loc=loc, **kwargs)


def multiplot(project, query, axes=('linear', 'linear'),
              columns_type=(float, str, float), normalize='', loc='upper left', **kwargs):
    data, labels = run_query(project, query, columns_type)
    graphs.multiplot(data, labels, axes, normalize=normalize, loc=loc, **kwargs)


def main():

    # decide whether to create the tables or not
    run_tables = False
    # name of the target dataset to create the tables and read from
    dataset = 'test'

    comments_query = """
    select
        length(body) as body_size,
        author,
        created_utc,
        subreddit,
        1 as comment,
        0 as submission,
    from
        [fh-bigquery:reddit_comments.2007],
        [fh-bigquery:reddit_comments.2008],
        [fh-bigquery:reddit_comments.2009],
        [fh-bigquery:reddit_comments.2010],
        [fh-bigquery:reddit_comments.2011],
        [fh-bigquery:reddit_comments.2012],
        [fh-bigquery:reddit_comments.2013],
        [fh-bigquery:reddit_comments.2014]
    where
        created_utc is not NULL
        and author <> '[deleted]'
        and not right(author, 4) = '_bot'
        and not right(author, 3) = 'Bot'
        and not lower(author) contains 'transcriber'
        and not lower(author) contains 'automoderator'
    """
    if run_tables:
        run_query('reddit-978',
                  comments_query,
                  iterate=None,
                  target='{}.comments'.format(dataset),
                  allowLargeResults=True)

    submissions_query = """
        select
        length(title) + length(selftext) as body_size,
        author,
        created_utc,
        subreddit,
        0 as comment,
        1 as submission,
    from
        [reddit_submissions.2007],
        [reddit_submissions.2008],
        [reddit_submissions.2009],
        [reddit_submissions.2010],
        [reddit_submissions.2011],
        [reddit_submissions.2012],
        [reddit_submissions.2013],
        [reddit_submissions.2014]
    where
        created_utc is not NULL
        and author <> '[deleted]'
        and not (right(author, 4) = '_bot' or right(author, 3) = 'Bot')
        and not lower(author) contains 'transcriber'
        and not lower(author) contains 'automoderator'
    """
    if run_tables:
        run_query('reddit-978',
                  submissions_query,
                  iterate=None,
                  target='{}.submissions'.format(dataset),
                  allowLargeResults=True)

    user_query = """
    select
        author,
        min(created_utc) as min_created_utc,
        2007 + integer((min(created_utc) - 1167609600)/(365.25*24*3600)) as year,
        max(created_utc) - min(created_utc) as time_active,
    from
        [{0}.comments],
        [{0}.submissions],
    group by author
    """.format(dataset)
    if run_tables:
        run_query('reddit-978',
                  user_query,
                  iterate=None,
                  target='{}.users'.format(dataset),
                  allowLargeResults=True)

    subreddit_query = """
    select
        subreddit,
        min(created_utc) as min_created_utc,
    from
        [{0}.comments],
        [{0}.submissions],
    group by subreddit
    """.format(dataset)
    if run_tables:
        run_query('reddit-978',
                  subreddit_query,
                  iterate=None,
                  target='{}.subreddits'.format(dataset),
                  allowLargeResults=True)

    joined_tables = """
    select
        t.c.body_size as body_size,
        t.c.author as author,
        t.c.created_utc as created_utc,
        2007 + integer((t.c.created_utc - 1167609600)/(365.25*24*3600/12))/12 as time,
        t.c.subreddit as subreddit,
        t.c.comment as comment,
        t.c.submission as submission,
        t.u.min_created_utc as u_min_created_utc,
        t.u.year as u_year,
        t.u.time_active as u_time_active,
        integer((t.c.created_utc - t.u.min_created_utc) / (365.25*24*3600/12))/12 as u_time,
        s.min_created_utc as s_min_created_utc,
    from
        (select * from
            (%s) c
            join each
            (%s) u
            on c.author = u.author) t
        join each
        (%s) s
        on c.subreddit = s.subreddit
    """
    user_query = '[{}.users]'.format(dataset)
    subreddit_query = '[{}.subreddits]'.format(dataset)

    comments_query = '[{}.comments]'.format(dataset)
    big_table = joined_tables % (comments_query, user_query, subreddit_query)
    if run_tables:
        run_query('reddit-978',
                  big_table,
                  iterate=None,
                  target='{}.comments_meta'.format(dataset),
                  allowLargeResults=True)

    submissions_query = '[{}.submissions]'.format(dataset)
    big_table = joined_tables % (submissions_query, user_query, subreddit_query)
    if run_tables:
        run_query('reddit-978',
                  big_table,
                  iterate=None,
                  target='{}.submissions_meta'.format(dataset),
                  allowLargeResults=True)

    n_queries = 0
    all_queries = []
    run_queries = [i for i in range(1, 14)]

    ##########################################################################
    # OVERVIEW ###############################################################
    ##########################################################################

    # 1
    n_queries += 1
    all_queries.append(n_queries)
    query = """
    SELECT time AS Time_, year, total AS Cumulative_number FROM
        (SELECT
            2007 + INTEGER((min_created_utc - 1167609600)/(365.25*24*3600/12))/12 AS TIME,
            "Cumulative number of users" AS YEAR,
            COUNT(*) AS total,
        FROM [{0}.users]
        group each by YEAR, TIME) users,
        (SELECT
            2007 + INTEGER((min_created_utc - 1167609600)/(365.25*24*3600/12))/12 AS TIME,
            "Cumulative number of subreddits" AS YEAR,
            COUNT(*) AS total,
        FROM [{0}.subreddits]
        group each by YEAR, TIME) subreddits
        ORDER BY YEAR, TIME
    """.format(dataset)
    if n_queries in run_queries:
        multiplot_cumulative('reddit-978',
                             query,
                             ('linear', 'log'),
                             loc='lower right',
                             show=False,
                             linewidth=3,
                             legend_size=15,
                             y_min=10,
                             y_max=20000000,
                             linestyle=['--', '-'],
                             filename='../images/cumulative_users_subreddits.eps')

    # 2
    n_queries += 1
    all_queries.append(n_queries)
    query = """
    SELECT time AS Time_, year, total AS Active_number FROM
        (SELECT
            time,
            "Number of active users" AS YEAR,
            COUNT(DISTINCT author, 100000) AS total,
        FROM [{0}.comments_meta], [{0}.submissions_meta]
        group each by YEAR, TIME),
        (SELECT
            time,
            "Number of active subreddits" AS YEAR,
            COUNT(DISTINCT subreddit, 100000) AS total,
        FROM [{0}.comments_meta], [{0}.submissions_meta]
        group each by YEAR, TIME)
        ORDER BY YEAR, TIME
    """.format(dataset)
    if n_queries in run_queries:
        multiplot('reddit-978',
                  query,
                  ('linear', 'log'),
                  loc='lower right',
                  show=False,
                  linewidth=3,
                  legend_size=15,
                  y_min=10,
                  y_max=20000000,
                  linestyle=['--', '-'],
                  filename='../images/active_users_subreddits.eps')

    ##########################################################################
    # USER ACTIVITY ##########################################################
    ##########################################################################

    # 3
    n_queries += 1
    all_queries.append(n_queries)
    query = """
    select
        time as Time_,
        'Number of posts per surviving user' as year,
        count(*)/count(distinct author) as Average_number_of_posts_per_user
    from [{0}.comments_meta], [{0}.submissions_meta]
    group each by Time_
    order by Time_
    """.format(dataset)
    if n_queries in run_queries:
        multiplot('reddit-978',
                  query,
                  legend=False,
                  show=False,
                  linewidth=3,
                  y_min=0,
                  y_max=31,
                  filename='../images/avr_posts_per_user_over_time_total.eps')

    # 4
    n_queries += 1
    all_queries.append(n_queries)
    query = """
    select
        u_time as Time_in_user_referential,
        'Overall' as year,
        count(*)/count(distinct author, 100000) as Average_number_of_posts_per_user
    from [{0}.comments_meta], [{0}.submissions_meta]
    group each by Time_in_user_referential
    having Time_in_user_referential <= 6
    order by Time_in_user_referential
    """.format(dataset)
    if n_queries in run_queries:
        multiplot('reddit-978',
                  query,
                  legend=False,
                  show=False,
                  linewidth=3,
                  y_min=0,
                  y_max=31,
                  filename='../images/avr_posts_per_user_user_ref_total.eps')

    # 5
    n_queries += 1
    all_queries.append(n_queries)
    query = """
    SELECT time AS Time_, year, avr_n_posts_per_user AS Average_number_of_posts_per_user FROM
    (SELECT
        time,
        STRING(u_year) AS YEAR,
        COUNT(*)/COUNT(DISTINCT author,100000) avr_n_posts_per_user
    FROM [{0}.comments_meta], [{0}.submissions_meta]
    WHERE
        u_year > 2007
        AND u_year < 2014
    group each by YEAR, TIME),
    (SELECT
        time,
        'Overall' AS YEAR,
        COUNT(*)/COUNT(DISTINCT author, 100000) AS avr_n_posts_per_user
    FROM [{0}.comments_meta], [{0}.submissions_meta]
    group each by TIME)
    ORDER BY YEAR, TIME
    """.format(dataset)
    if n_queries in run_queries:
        multiplot('reddit-978',
                  query,
                  legend_size=15,
                  show=False,
                  y_min=0,
                  y_max=32,
                  linewidth=3,
                  linestyle=['-', ':', '--', '-', ':', '--', '-'],
                  filename='../images/avr_posts_per_user_over_time_cohorts.eps')

    # 6
    n_queries += 1
    all_queries.append(n_queries)
    query = """
    SELECT time AS Time_in_user_referential, year, avr_n_posts_per_user AS Average_number_of_posts_per_user FROM
    (SELECT
        u_time AS time,
        string(u_year) AS year,
        count(*)/count(DISTINCT author,100000) avr_n_posts_per_user
    FROM [{0}.comments_meta], [{0}.submissions_meta]
    WHERE
        u_year > 2007
        AND u_year < 2014
    group each by YEAR, TIME
    HAVING TIME <= 2014 - INTEGER(YEAR)),
    ORDER BY YEAR, TIME
    """.format(dataset)
    if n_queries in run_queries:
        multiplot('reddit-978',
                  query,
                  loc='lower right',
                  legend_size=15,
                  show=False,
                  y_min=0,
                  y_max=32,
                  linewidth=3,
                  linestyle=['-', ':', '--', '-', ':', '--', '-'],
                  filename='../images/avr_posts_per_user_cohorts.eps')

    # 7
    n_queries += 1
    all_queries.append(n_queries)
    query = """
    select
        u_time as Time_in_user_referential,
        integer(u_time_active/(365.25*24*3600)) as year,
        count(*)/count(distinct author,100000) Average_number_of_posts_per_user
    from [{0}.comments_meta], [{0}.submissions_meta]
    where
        u_year = {1}
    group each by year, Time_in_user_referential
    order by year, Time_in_user_referential
    """
    if n_queries in run_queries:
        for year in range(2010, 2013):
            multiplot('reddit-978',
                      query.format(dataset, year),
                      ('linear', 'linear'),
                      loc='lower right',
                      linewidth=4,
                      show=False,
                      x_label_size=24,
                      y_label_size=24,
                      tick_size=24,
                      legend_size=20,
                      y_min=0,
                      y_max=32,
                      linestyle=['-', ':', '--', '-', ':', '--', '-'],
                      filename='../images/avr_posts_per_user_for_surviving_year_for_{}.eps'.format(year))

    ##########################################################################
    # EFFORT PER POST ########################################################
    ##########################################################################

    # 8
    n_queries += 1
    all_queries.append(n_queries)
    query = """
    SELECT time AS Time_, year, avr_comment_size AS Average_comment_length FROM
    (SELECT
        time,
        STRING(u_year) AS YEAR,
        sum(body_size)/COUNT(*) avr_comment_size
    FROM [{0}.comments_meta]
    WHERE
        u_year > 2007
        AND u_year < 2014
    group each by YEAR, TIME
    ORDER BY YEAR, TIME),
    (SELECT
        time,
        'Overall' AS YEAR,
        sum(body_size)/COUNT(*) avr_comment_size
    FROM [{0}.comments_meta]
    group each by YEAR, TIME
    ORDER BY YEAR, TIME)
    """.format(dataset)
    if n_queries in run_queries:
        multiplot('reddit-978',
                  query,
                  loc='lower left',
                  legend_size=15,
                  show=False,
                  linewidth=3,
                  y_min=158,
                  y_max=242,
                  linestyle=['-', ':', '--', '-', ':', '--', '-'],
                  filename='../images/avr_comment_size_over_time_cohorts.eps')

    # 9
    n_queries += 1
    all_queries.append(n_queries)
    query = """
    SELECT time AS Time_in_user_referential, year, avr_comment_size AS Average_comment_length FROM
    (SELECT
        u_time AS time,
        string(u_year) AS year,
        sum(body_size)/count(*) avr_comment_size
    FROM [{0}.comments_meta]
    WHERE
        u_year > 2007
        AND u_year < 2014
    group each by YEAR, TIME
    HAVING TIME <= 2014 - INTEGER(YEAR)),
    ORDER BY YEAR, TIME
    """.format(dataset)
    if n_queries in run_queries:
        multiplot('reddit-978',
                  query,
                  loc='lower right',
                  legend_size=15,
                  show=False,
                  linewidth=3,
                  y_min=158,
                  y_max=242,
                  linestyle=['-', ':', '--', '-', ':', '--', '-'],
                  filename='../images/avr_comment_size_cohorts.eps')

    # 10
    n_queries += 1
    all_queries.append(n_queries)
    query = """
    select
        u_time as Time_in_user_referential,
        integer(u_time_active/(365.25*24*3600)) as year,
        sum(body_size)/count(*) as Average_comment_length,
    from [{0}.comments_meta]
    where
        u_year = {1}
    group each by year, Time_in_user_referential
    order by year, Time_in_user_referential
    """
    if n_queries in run_queries:
        for year in range(2010, 2013):
            multiplot('reddit-978',
                      query.format(dataset, year),
                      ('linear', 'linear'),
                      loc='lower right',
                      linewidth=4,
                      show=False,
                      x_label_size=24,
                      y_label_size=24,
                      tick_size=24,
                      legend_size=20,
                      y_min=158,
                      y_max=230,
                      linestyle=['-', ':', '--', '-', ':', '--', '-'],
                      filename='../images/avr_comment_length_for_surviving_year_for_{}.eps'.format(year))

    ##########################################################################
    # COMMENTS PER SUBMISSIONS ###############################################
    ##########################################################################

    # 11
    n_queries += 1
    all_queries.append(n_queries)
    query = """
    SELECT time AS Time_, year, comments_per_submission AS Comments_per_submission FROM
    (SELECT
        time,
        STRING(u_year) AS YEAR,
        sum(COMMENT)/sum(submission) AS comments_per_submission
    FROM [{0}.comments_meta], [{0}.submissions_meta]
    WHERE
        u_year > 2007
        AND u_year < 2014
    group each by YEAR, TIME),
    (SELECT
        time,
        'Overall' AS YEAR,
        sum(COMMENT)/sum(submission) AS comments_per_submission
    FROM [{0}.comments_meta], [{0}.submissions_meta]
    WHERE
        u_year > 2007
        AND u_year < 2014
    group each by YEAR, TIME)
    ORDER BY YEAR, TIME
    """.format(dataset)
    if n_queries in run_queries:
        multiplot('reddit-978',
                  query,
                  ('linear', 'linear'),
                  show=False,
                  y_min=0,
                  y_max=19,
                  linewidth=3,
                  legend_size=15,
                  linestyle=['-', ':', '--', '-', ':', '--', '-'],
                  filename='../images/comments_per_submissions_over_time_cohorts.eps')

    # 12
    n_queries += 1
    all_queries.append(n_queries)
    query = """
    SELECT time AS Time_in_user_referential, year, comments_per_submission AS Comments_per_submission FROM
    (SELECT
        u_time AS time,
        string(u_year) AS year,
        sum(comment)/sum(submission) AS comments_per_submission
    FROM [{0}.comments_meta], [{0}.submissions_meta]
    WHERE
        u_year > 2007
        AND u_year < 2014
    group each by YEAR, TIME
    HAVING TIME <= 2014 - INTEGER(YEAR)),
    ORDER BY YEAR, TIME
    """.format(dataset)
    if n_queries in run_queries:
        multiplot('reddit-978',
                  query,
                  ('linear', 'linear'),
                  loc='lower right',
                  show=False,
                  y_min=0,
                  y_max=19,
                  linewidth=3,
                  legend_size=15,
                  linestyle=['-', ':', '--', '-', ':', '--', '-'],
                  filename='../images/comments_per_submissions_cohorts.eps')

    # 13
    n_queries += 1
    all_queries.append(n_queries)
    query = """
    select
        u_time as Time_in_user_referential,
        integer(u_time_active/(365.25*24*3600)) as year,
        sum(comment)/sum(submission) as Comments_per_submission,
    from [{0}.comments_meta], [{0}.submissions_meta]
    where
        u_year = {1}
    group each by year, Time_in_user_referential
    order by year, Time_in_user_referential
    """
    if n_queries in run_queries:
        for year in range(2008, 2012):
            multiplot('reddit-978',
                      query.format(dataset, year),
                      ('linear', 'linear'),
                      loc='lower right',
                      linewidth=4,
                      show=False,
                      x_label_size=30,
                      y_label_size=30,
                      tick_size=28,
                      legend_size=24,
                      y_min=0,
                      y_max=20,
                      linestyle=['-', ':', '--', '-', ':', '--', '-'],
                      filename='../images/comments_per_submissions_for_surviving_year_for_{}.eps'.format(year))


if __name__ == '__main__':
    main()
