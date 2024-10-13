import json
from collections import Counter

def load_json_file(file_path):
    try:
        with open(file_path, 'r') as f:
            content = f.read()
            if not content.strip():
                print(f"Error: File '{file_path}' is empty.")
                return None
            return json.loads(content)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON in '{file_path}': {str(e)}")
        print(f"First 100 characters of the file: {content[:100]}")
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
    except Exception as e:
        print(f"Unexpected error when reading '{file_path}': {str(e)}")
    return None

# Load data
fox_data = load_json_file('fox_news.json')
cnn_data = load_json_file('cnn.json')

if fox_data is None or cnn_data is None:
    print("Error loading data. Please check the JSON files and try again.")
    exit(1)

# defining keyword pairs
keyword_pairs = [
    ("Safety", "Risk"),
    ("Public Health", "Individual Choice"),
    ("Scientific Consensus", "Uncertainty"),
    ("Social Responsibility", "Personal Freedom"),
    ("Individual Liberty", "Collective Responsibility"),
    ("Limited Government", "Public Health Mandate"),
    ("Personal Choice", "Societal Obligation"),
    ("Free Market Solutions", "Government Interventions"),
    ("Traditional Values", "Progressive Policies")
]

def vanilla_nli(comment, keyword1, keyword2):
    '''
    Simple NLI function to classify the relationship between a comment and a keyword pair
    '''
    text = comment['text'].lower()

    if keyword1.lower() in text and keyword2.lower() not in text:
        return 'entailment', keyword1
    elif keyword2.lower() in text and keyword1.lower() not in text:
        return 'entailment', keyword2
    elif keyword1.lower() in text and keyword2.lower() in text:
        return 'neutral', None
    else:
        return 'unrelated', None


def analyze_comments(data, keyword_pairs):
    results = []
    for comment in data['comments']:
        for pair in keyword_pairs:
            relation, keyword = vanilla_nli(comment, pair[0], pair[1])

            if relation != 'unrelated':
                results.append((pair, relation, keyword))
    return results

fox_news_results = analyze_comments(fox_data, keyword_pairs)
cnn_results = analyze_comments(cnn_data, keyword_pairs)

def summarize_results(results):
    summary = Counter()
    for pair, relation, keyword in results:
        if relation == 'entailment':
            summary[keyword] += 1
    return summary

print("Fox News Summary:")
print(summarize_results(fox_news_results))

print("\nCNN Summary:")
print(summarize_results(cnn_results))
