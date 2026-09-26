import re

def convert_test_format(input_text):
    blocks = re.split(r'\n\+\+\+\+\n', input_text.strip())
    output_lines = []
    
    for block in blocks:
        if not block.strip():
            continue
        
        lines = block.strip().split('\n')
        question = lines[0].strip()
        
        variants = []
        current_variant = []
        answer = None
        
        for line in lines[1:]:
            if line.startswith('===='):
                if current_variant:
                    variant_text = ' '.join(current_variant).strip()
                    if variant_text.startswith('#'):
                        answer = variant_text[1:].strip()
                        variant_text = answer
                    variants.append(variant_text)
                    current_variant = []
            else:
                current_variant.append(line)
        
        if current_variant:
            variant_text = ' '.join(current_variant).strip()
            if variant_text.startswith('#'):
                answer = variant_text[1:].strip()
                variant_text = answer
            variants.append(variant_text)
        
        # Ensure we have exactly 4 variants
        while len(variants) < 4:
            variants.append("")
        
        output_lines.append(question)
        output_lines.append(f"A. {variants[0] if variants[0] else 'Yo‘q'}")
        output_lines.append(f"B. {variants[1] if variants[1] else 'Yo‘q'}")
        output_lines.append(f"C. {variants[2] if variants[2] else 'Yo‘q'}")
        output_lines.append(f"D. {variants[3] if variants[3] else 'Yo‘q'}")
        
        if answer:
            answer_letter = None
            for i, v in enumerate(variants):
                if v == answer:
                    answer_letter = ['A', 'B', 'C', 'D'][i]
                    break
            if answer_letter:
                output_lines.append(f"ANSWER: {answer_letter}")
            else:
                output_lines.append("ANSWER: A")  # default
        else:
            output_lines.append("ANSWER: A")  # default
        
        output_lines.append("")  # blank line between questions
    
    return '\n'.join(output_lines)

# Read input file
with open('input.txt', 'r', encoding='utf-8') as f:
    input_text = f.read()

# Convert format
converted_text = convert_test_format(input_text)

# Write output file
with open('converted_test.txt', 'w', encoding='utf-8') as f:
    f.write(converted_text)

print("Conversion completed. Check 'converted_test.txt'")
