import logging
import json
import sys
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import nltk
from nltk.corpus import stopwords
import re

# Ensure you have the NLTK stopwords dataset downloaded
nltk.download('stopwords')
stop_words = list(set(stopwords.words('english')))

# Set up logging
logging.basicConfig(level=logging.DEBUG, handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
for handler in logger.handlers:
    handler.setLevel(logging.DEBUG)

logger.info("Starting script...")

causal_phrases = [
    'because', 'due to', 'as a result', 'therefore', 'thus', 'consequently',
    'hence', 'so that', 'caused by', 'resulted in', 'leading to', 'since'
]

def detect_causal_relationships(contents):
    causal_pairs = []
    for idx, content in enumerate(contents):
        for phrase in causal_phrases:
            if phrase in content.lower():
                causal_pairs.append({
                    "id": str(idx),  # Ensure id is a string
                    "phrase": phrase,
                    "context": content
                })
                logger.debug(f"Found causal phrase '{phrase}' in content with id {idx}")
    return causal_pairs

def detect_hierarchical_relationships(contents):
    hierarchical_pairs = []
    heading_patterns = [
        re.compile(r'^\d+\.\s+.+'),  # Matches headings like "1. Introduction"
        re.compile(r'^[A-Za-z]+\.\s+.+'),  # Matches headings like "A. Background"
    ]
    for idx, content in enumerate(contents):
        for pattern in heading_patterns:
            if pattern.match(content):
                hierarchical_pairs.append({
                    "id": str(idx),  # Ensure id is a string
                    "heading": content
                })
                logger.debug(f"Found hierarchical heading in content with id {idx}: {content}")
    return hierarchical_pairs

def vectorize_and_find_similar(nodes, threshold=0.8, keyword_threshold=0.2):
    logger.info("Loading BERT model and tokenizer...")
    model = SentenceTransformer('paraphrase-MiniLM-L6-v2')

    # Filter out nodes with invalid or missing content
    valid_nodes = [node for node in nodes if node.get('content') and node.get('id')]
    contents = [node['content'] for node in valid_nodes]

    logger.info("Vectorizing node contents...")
    embeddings = model.encode(contents)

    # Use TF-IDF for keyword extraction
    vectorizer = TfidfVectorizer(stop_words=stop_words)
    tfidf_matrix = vectorizer.fit_transform(contents)
    keywords = vectorizer.get_feature_names_out()

    num_nodes = len(valid_nodes)
    similar_pairs = []
    keyword_pairs = []
    causal_pairs = detect_causal_relationships(contents)
    hierarchical_pairs = detect_hierarchical_relationships(contents)

    logger.info("Calculating cosine similarities...")
    for i in range(num_nodes):
        for j in range(i + 1, num_nodes):
            sim = cosine_similarity([embeddings[i]], [embeddings[j]])[0][0]
            if sim > threshold:
                similar_pairs.append({
                    "start_id": str(valid_nodes[i]['id']),  # Ensure id is a string
                    "end_id": str(valid_nodes[j]['id']),    # Ensure id is a string
                    "similarity": float(sim)  # Convert to standard float
                })
                logger.debug(f"Found similar pair: {valid_nodes[i]['id']} and {valid_nodes[j]['id']} with similarity {sim}")

    logger.info("Extracting and filtering keywords...")
    for i in range(num_nodes):
        for j in range(i + 1, num_nodes):
            common_keywords = set(tfidf_matrix[i].nonzero()[1]) & set(tfidf_matrix[j].nonzero()[1])
            common_keywords = {keywords[k] for k in common_keywords if tfidf_matrix[i, k] > keyword_threshold and tfidf_matrix[j, k] > keyword_threshold and keywords[k] not in stop_words}
            if common_keywords:
                keyword_pairs.append({
                    "start_id": str(valid_nodes[i]['id']),  # Ensure id is a string
                    "end_id": str(valid_nodes[j]['id']),    # Ensure id is a string
                    "keywords": list(common_keywords)
                })
                logger.debug(f"Found keyword overlap: {valid_nodes[i]['id']} and {valid_nodes[j]['id']} with keywords {common_keywords}")

    return similar_pairs, keyword_pairs, causal_pairs, hierarchical_pairs

if __name__ == "__main__":
    logger.info("Reading input data...")
    try:
        input_file_path = sys.argv[1]
        output_file_path = sys.argv[2]
        with open(input_file_path, 'r') as f:
            input_data = json.load(f)
        logger.info(f"Input data received: {input_data}")
    except Exception as e:
        logger.error(f"Error reading input data: {e}")
        sys.exit(1)

    logger.info("Finished reading input data")

    similar_pairs, keyword_pairs, causal_pairs, hierarchical_pairs = vectorize_and_find_similar(input_data)

    output = {
        "similar_pairs": similar_pairs,
        "keyword_pairs": keyword_pairs,
        "causal_pairs": causal_pairs,
        "hierarchical_pairs": hierarchical_pairs
    }

    logger.info(f"Writing results to file: {output_file_path}")
    try:
        with open(output_file_path, 'w') as f:
            json.dump(output, f, indent=4)
    except Exception as e:
        logger.error(f"Error writing output data: {e}")
        sys.exit(1)
