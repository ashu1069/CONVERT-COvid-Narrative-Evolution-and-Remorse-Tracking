import pandas as pd
import spacy
from collections import Counter
from pathlib import Path
from nltk.corpus import wordnet
from spacy.tokens import Token
import glob
import json
from datetime import datetime

# Register the 'synonyms' extension
def get_synonyms(token):
    synonyms = set()
    for syn in wordnet.synsets(token.text):
        for lemma in syn.lemmas():
            synonyms.add(lemma.name())
    return synonyms

Token.set_extension('synonyms', getter=get_synonyms, force=True)

nlp = spacy.load("en_core_web_sm")

# Define keyword pairs
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

def enhanced_nli(comment_text, keyword1, keyword2):
    '''
    Classify the relationship between a comment and a keyword pair
    '''
    text = str(comment_text).lower()
    doc = nlp(text)

    # Create set of tokens and their lemmas
    text_tokens = set(token.text for token in doc)
    text_lemmas = set(token.lemma_ for token in doc)

    # Create set of keywords and their lemmas
    keyword1_tokens = set(token.text for token in nlp(keyword1))
    keyword1_lemmas = set(token.lemma_ for token in nlp(keyword1))
    keyword2_tokens = set(token.text for token in nlp(keyword2))
    keyword2_lemmas = set(token.lemma_ for token in nlp(keyword2))

    # Check if any keyword is in the text
    keyword1_match = any(kw in text_tokens or kw in text_lemmas or any(kw in token for token in text_tokens) 
                        for kw in keyword1_tokens | keyword1_lemmas)
    keyword2_match = any(kw in text_tokens or kw in text_lemmas or any(kw in token for token in text_tokens) 
                        for kw in keyword2_tokens | keyword2_lemmas)

    if keyword1_match and not keyword2_match:
        return 'entailment', keyword1
    elif keyword2_match and not keyword1_match:
        return 'entailment', keyword2
    elif keyword1_match and keyword2_match:
        return 'neutral', None
    else:
        for token in doc:
            if any(syn.lower() in keyword1.lower() for syn in token._.synonyms):
                return 'entailment', keyword1
            elif any(syn.lower() in keyword2.lower() for syn in token._.synonyms):
                return 'entailment', keyword2

    return 'neutral', None

def analyze_csv_file(file_path):
    """
    Analyze a single CSV file and return results
    """
    try:
        # Read CSV file
        df = pd.read_csv(file_path)
        results = []
        
        # Process each comment
        for _, row in df.iterrows():
            comment_text = row['text']
            comment_id = row['commentId']
            published_at = row['publishedAt']
            
            for pair in keyword_pairs:
                relation, keyword = enhanced_nli(comment_text, pair[0], pair[1])
                if relation != 'neutral':
                    results.append({
                        'comment_id': comment_id,
                        'published_at': published_at,
                        'keyword_pair': pair,
                        'relation': relation,
                        'matched_keyword': keyword,
                        'comment_text': comment_text
                    })
        
        return results
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return []

def process_csv_folder(folder_path):
    """
    Process all CSV files in a folder and generate analysis
    """
    # Get all CSV files in the folder
    csv_files = glob.glob(str(Path(folder_path) / "*.csv"))
    
    all_results = []
    file_summaries = {}
    
    # Process each file
    for file_path in csv_files:
        file_name = Path(file_path).stem
        print(f"Processing {file_name}...")
        
        results = analyze_csv_file(file_path)
        all_results.extend(results)
        
        # Create summary for this file
        summary = Counter()
        for result in results:
            if result['relation'] == 'entailment':
                summary[result['matched_keyword']] += 1
        
        file_summaries[file_name] = dict(summary)
    
    # Create overall summary
    overall_summary = Counter()
    for result in all_results:
        if result['relation'] == 'entailment':
            overall_summary[result['matched_keyword']] += 1
    
    # Prepare output
    analysis_results = {
        'overall_summary': dict(overall_summary),
        'file_summaries': file_summaries,
        'detailed_results': all_results
    }
    
    # Save results to JSON file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'nli_analysis_results_{timestamp}.json'
    with open(output_file, 'w') as f:
        json.dump(analysis_results, f, indent=4)
    
    return analysis_results

def print_analysis_summary(analysis_results):
    """
    Print a formatted summary of the analysis results
    """
    print("\n=== Overall Summary ===")
    for keyword, count in analysis_results['overall_summary'].items():
        print(f"{keyword}: {count}")
    
    print("\n=== File-by-File Summary ===")
    for file_name, summary in analysis_results['file_summaries'].items():
        print(f"\n{file_name}:")
        for keyword, count in summary.items():
            print(f"  {keyword}: {count}")

# Example usage
if __name__ == "__main__":
    folder_path = "extracted_text_cnn"  # Replace with your folder path
    results = process_csv_folder(folder_path)
    print_analysis_summary(results)