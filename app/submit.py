from pyspark.sql import SparkSession

def word_count(text):
    """Counts the number of words in a given text.

    Args:
        text (str): The text to count words in.

    Returns:
        pyspark.sql.DataFrame: A DataFrame containing word counts.
    """

    spark = SparkSession.builder.appName("Word Count").getOrCreate()

    # Create an RDD from the text
    text_rdd = spark.sparkContext.parallelize([text])

    # Split the text into words
    words_rdd = text_rdd.flatMap(lambda line: line.split())

    # Count word occurrences
    word_counts = words_rdd.countByValue()

    # Convert word counts to a DataFrame
    word_counts_rdd = spark.sparkContext.parallelize(word_counts.items())
    word_counts_df = word_counts_rdd.toDF(["word", "count"])

    return word_counts_df

if __name__ == "__main__":
    long_text = """Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."""

    result_df = word_count(long_text)
    result_df.show()