import pandas as pd
import re
import csv

from openai import AzureOpenAI

client = AzureOpenAI(
    api_key="ca4a4f91ec8445d2a24a62ba134e0ef5",
    azure_endpoint="https://onto-openai.openai.azure.com/",
    api_version="2023-12-01-preview"
)
# Functions to prepare input for llm processing
def capture_age_blocks(input_text):
    """
    Standardised blocks defining age make parsing way more difficult - removed
    """
    start_sentence = "Are the trial subjects under 18?"
    end_sentence = "F.1.3.1 Number of subjects for this age range"
    regex_pattern = re.escape(start_sentence) + r'([\s\S]*?)' + re.escape(end_sentence)
    matches = re.findall(regex_pattern, input_text)

    captured_blocks = []

    for match in matches:
        block_text = start_sentence + match + end_sentence
        captured_blocks.append(block_text)
    captured_blocks = list(set(captured_blocks))
    remaining_text = re.sub(regex_pattern, '', input_text)

    return remaining_text.strip(), captured_blocks

def preprocess_raw(text_raw):
    """
    Processes raw text and finds inclusion criteria block that is most suitable for splitting:
    one containing more than one paragraph is more suitable
    """
    # If one version:
    if "|" not in text_raw:
        value, captured_age_blocks = capture_age_blocks(text_raw)
        return value

    # Else: Split text by "|"
    sections = text_raw.split('|')
    # Check for the first element with more than one paragraph
    for section in sections:
        value, captured_age_blocks = capture_age_blocks(section)
        paragraphs = value.strip().split('\n')
        if len(paragraphs) > 1:
            return value

    # If no element with more than one paragraph, return the first one
    value, captured_age_blocks = capture_age_blocks(sections[0])
    return value

# LLM Processing
def parse_criteria(input_tekst):
    system_message = """
# Task: Organize and Structure Eligibility Criteria for a Clinical Trial. Language model should adhere strictly to formatting and organization guidelines.
   
## Steps:

### 1. Parse Text to identify titles/subtitles, and criteria:
   - Read through the eligibility criteria text block thoroughly. One input contains criteria for a single study, which may, or may not be split according to groups/cohorts/study parts, etc.
   - Recognize titles/sub-titles e.g. of groups/cohorts/study parts that are mentioned in the text (if none are mentioned in the text - don't make them up) (first line may contain parsing artifacts)
   - Recognize notes remarks and other text strange text
   - Parse text to break it down into separate criteria and sub-criteria (restrict to parsing existing text, don't add or remove text)
   - Any keywords such as or (OR), and (AND) and either (EITHER) should absolutely be kept attached to the crtieria they belonf to.
   - Length of input and output text (without numbering) should be equal
   
### 2. Criteria formatting:
    - Reformat the text to be one single line without any newlines per criterion
    - Do not rephrase or adapt the text
    - Do not delete or add any text
    - Importantly words like "or", "and", "either" should be kept. If such words are encountered on a separate line, add them to previous line  
    - Ensure that if criteria are split, their meaning is not altered, e.g. negation should not be lost by splitting
    - Avoid adding any text or removing any text, strictly limit yourself to parsing and reformatting.

### 3. Identify Hierarchy:
   - Recognize hierarchy between the titles/subtitles and criteria and notes and remarks based on sub-bullet points, numbering, indentation (indent of the first line may occasionally be stripped by previous parsing). 
   - Up to 10 levels of hierarchy may exist.
   
### 4. Numbering System:
   - Establish consistent numbering for each line.
   - Use nested numbering for sections within the hierarchy:
        - If the group is "2," its criteria could be "2.1," "2.2," etc, and sub-criteria, notes and remarks could be "2.1.1" and "2.1.2", etc.
        - Besides numbering, use indentation to signify hierarchy level
   - Consistency Across Sections: Ensure that the numbering style is consistent and continuous across different sections and subsections of the document. Avoid mixing different numbering formats within the same level of hierarchy.
   - EACH LINE MUST HAVE A NUMBER. NOT A BULLETPOINT, NOT A DASH, A NUMBER. NO LINES MAY BE UNNUMBERED. NOTES AND REMARKS ARE SUB-CATEGORIES.
   - EVEN LINES THAT DID NOT HAVE A PLACE IN THE HIERARCHY, MUST NOW GET A NUMBER. If needed, change original hierarchy.
   
### 5. Output Formatting:
   - Output parsed reformatted text, always foreseen with new numbering.
      - Ensure standardised, uniform and numbered structure. This will facilitate further parsing of the text by data processing scripts.
      - Ensure that there are no empty lines in the output regardless of formatting considerations to facilitate parsing.
      - Each line must be numbered according to its hierarchy. This is essential for parsing.
      - All identified titles or sub-titles notes and remarks etc. that have been found in step 1 must:
            - Be numbered and be part of one single hierarchy across entire document
   - Include all parts of the original text in the output.

   
Example 1: Input
Group 1
- Signed Written Informed Consent
● Provision of signed and dated, written informed consent
- Age and Sex
● Females should agree to use adequate contraceptive measure, should not be breast feeding and must have a negative pregnancy test or must have evidence of non-child-bearing potential by fulfilling one of following criteria at screening:
    - Post-menopausal defined as aged more than 50 years and ameorrhoeic for at least 12 months following cessation of all exogenous hormonal treatments
    - Documentation of irreversible surgical sterilization by hysterectomy, bilateral oophorectomy or bilateral salpingectomy but not tubal ligation.

Part 2
- Age and Sex
● Males should agree to use adequate contraceptive measure

Example 1: Output
1. Group 1
    1.1 Signed Written Informed Consent
        1.1.1 Provision of signed and dated, written informed consent
    1.2 Age and Sex
        1.2.2 Females should agree to use adequate contraceptive measure, should not be breast feeding and must have a negative pregnancy test or must have evidence of non-child-bearing potential by fulfilling one of following criteria at screening:
            1.2.2.1 Post-menopausal defined as aged more than 50 years and ameorrhoeic for at least 12 months following cessation of all exogenous hormonal treatments
            1.2.2.2 Documentation of irreversible surgical sterilization by hysterectomy, bilateral oophorectomy or bilateral salpingectomy but not tubal ligation.
2. Part 2
    2.1 Age and Sex
        2.1.1 Males should agree to use adequate contraceptive measure

Example 2: Input
    - have type 2 diabetes as defined by World Health Organization (WHO) criteria
    - are taking oral anti-hyperglycemic medications (OAMs) and are judged as OAM failure by the investigator

Example 2: Output
    1. have type 2 diabetes as defined by World Health Organization (WHO) criteria
    2. are taking oral anti-hyperglycemic medications (OAMs) and are judged as OAM failure by the investigator

Example 3: Input
* 		Sign and date the Informed Consent Form (ICF), prior to the start of any study-specific qualification procedures.
* 		Adults ≥ 18 years of age on the day of signing the ICF.
Additional inclusion criteria for Part 1:
* Has a histologically or cytologically documented locally advanced metastatic cancers
Additional inclusion criteria for Part 2:
* Has a histologically or cytologically documented locally advanced metastatic cancers
* Is able to provide either of the following baseline tumor samples:
	a. Fresh core needle biopsy samples obtained during the Screening Period, or
	b. Alternative FFPE tumor tissue samples obtained by biopsy or surgery performed after the completion date


Example 3: Output
1. Sign and date the Informed Consent Form (ICF), prior to the start of any study-specific qualification procedures.
2. Adults ≥ 18 years of age on the day of signing the ICF.
3. Additional inclusion criteria for Part 1:
	3.1 Has a histologically or cytologically documented locally advanced metastatic cancers
4. Additional inclusion criteria for Part 2:
	4.1 Has a histologically or cytologically documented locally advanced metastatic cancers
	4.2 Is able to provide either of the following baseline tumor samples:
		4.2.1 Fresh core needle biopsy samples obtained during the Screening Period, or
		4.2.2 Alternative FFPE tumor tissue samples obtained by biopsy or surgery performed after the completion date

    
Example 4: Input
Cohort 1
- Male and female patients 18 years or older (at the time of the screening visit)
- Presence of NASH as demonstrated by ONE of the following:
EITHER:
1)  Histologic evidence of NASH based on liver biopsy obtained 2 years or less before randomization with a diagnosis consistent with NASH, fibrosis level F1, F2 or F3, in the absence of a histological diagnosis of alternative chronic liver diseases AND ALT ≥ 60 IU/L (males) or ≥ 40 IU/L (females)
OR
2) Phenotypic diagnosis of NASH based on the presence of ALL THREE of the following:
- ALT ≥ 60 IU/L (males) or ≥ 40 IU/L (females) AND
- BMI ≥ 27 kg/m2 (in patients with a self-identified race other than Asian)  or ≥23 kg/m2 (in patients with a self-identified Asian race) AND
- Diagnosis of Type 2 diabetes mellitus by having either: HbA1C ≥ 6.5% OR Symptoms of diabetes plus hyperglycemia as indicated by fasting plasma glucose ≥126 mg/dl (≥ 7.0 mmol/l), two hour plasma glucose concentration ≥200mg/dl (≥11.1 mmol/l, two hours after 75g anhydrous glucose in an oral glucose tolerance test (OGTT)
OR
Drug therapy for Type 2 diabetes mellitus

Example 4: Output
1. Cohort 1
    1.1 Male and female patients 18 years or older (at the time of the screening visit)
    1.2. Presence of NASH as demonstrated by ONE of the following: EITHER
       1.2.1 Histologic evidence of NASH based on liver biopsy obtained 2 years or less before randomization with a diagnosis consistent with NASH, fibrosis level F1, F2 or F3, in the absence of a histological diagnosis of alternative chronic liver diseases AND ALT ≥ 60 IU/L (males) or ≥ 40 IU/L (females) OR
       1.2.2 Phenotypic diagnosis of NASH based on the presence of ALL THREE of the following:
           1.2.2.1 ALT ≥ 60 IU/L (males) or ≥ 40 IU/L (females) AND
           1.2.2.2 BMI ≥ 27 kg/m2 (in patients with a self-identified race other than Asian) or ≥ 23 kg/m2 (in patients with a self-identified Asian race) AND
           1.2.2.3 Diagnosis of Type 2 diabetes mellitus by having either: 
             1.2.2.3.1 HbA1C ≥ 6.5% OR 
             1.2.2.3.2 Symptoms of diabetes plus hyperglycemia as indicated by fasting plasma glucose ≥ 126 mg/dl (≥ 7.0 mmol/l), two hour plasma glucose concentration ≥ 200mg/dl (≥ 11.1 mmol/l, two hours after 75g anhydrous glucose in an oral glucose tolerance test (OGTT) OR
    1.3 Drug therapy for Type 2 diabetes mellitus
"""

    messages = [
        {"role": "system", "content": system_message},
        {"role": "user", "content": input_tekst}]

    result = client.chat.completions.create(
        model = "gpt-35-turbo-16k",
        messages = messages,
        temperature = 0.9
    )

    #print(result)
    print("==========================================================")
    print(input_tekst)
    print("==============")
    print(result.choices[0].message.content)
    print("==============")
    print(result.usage.prompt_tokens)
    print(result.usage.completion_tokens)
    print("==========================================================")
    return(result.choices[0].message.content)

# Wrappers
def process_column(text_raw):
    text = preprocess_raw(text_raw)
    processed_text = parse_criteria(text)
    return processed_text

def process_chunk(chunk, column_to_process, output_column_name):
    """
    This function applies the process_data function to each value in the specified column for a chunk.
    """
    chunk[output_column_name] = chunk[column_to_process].apply(process_column)
    return chunk

def process_csv(input_file, output_file, column_to_process, output_column_name, chunksize):
    # Iterate over chunks of the CSV file
    for chunk in pd.read_csv(input_file, sep=';', chunksize=chunksize):
        print('========================================================================================================')
        # Process the specified column for each chunk
        chunk = process_chunk(chunk, column_to_process, output_column_name)
        # Append the processed chunk to the output CSV file
        chunk.to_csv(output_file, index=False, mode='a', header=not chunksize)


if __name__ == "__main__":
    input_file = '/Users\quangrya\OneDrive - Boehringer Ingelheim\Desktop\Selection_5000_ClinicalStudies_v3.csv'
    output_file = '/Users\quangrya\OneDrive - Boehringer Ingelheim\Desktop\ai_split_5000.csv'  # Specify the output Excel file
    column_to_process = "Inclusion Criteria"  # Specify the column to be processed
    output_column_nme = "Round 1"
    # Specify the chunk size (number of rows to read at a time)
    # Set it to None to read the entire file into memory at once
    chunksize = 5  # Adjust this value based on your memory constraints

    with open(input_file, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        original_columns = next(reader)
    updated_columns = original_columns + [output_column_name]
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(updated_columns)

    process_csv(input_file, output_file, column_to_process, output_column_name, chunksize)

print("Parsing completed. Parsed results saved in output file.")

















