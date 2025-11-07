import cv2
import pytesseract
import re


IMG_PATH = "teste_snic.jpg"

def extrair_numero(img_path, linha="Sudeste", coluna="2025"):
  # prepara imagem
    img = cv2.imread(img_path)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    gray = cv2.resize(gray, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)
    _, th = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    
    data = pytesseract.image_to_data(th, lang="por", config="--oem 3 --psm 6", output_type=pytesseract.Output.DICT)
    
    tokens = []
    for i, text in enumerate(data["text"]):
        text = text.strip()
        if not text:
            continue
        x, y, w, h = data["left"][i], data["top"][i], data["width"][i], data["height"][i]
        tokens.append({"text": text, "cx": x + w/2, "cy": y + h/2})

 
    sudeste = min([t for t in tokens if t["text"].lower() == linha.lower()], key=lambda t: t["cy"])
    col2025 = min([t for t in tokens if t["text"].strip() == coluna], key=lambda t: t["cy"])


    numeros = [t for t in tokens if re.match(r"^\d{3,5}$|^\d{1,3}\.\d{3}$|^\d{1,3}\.\d{3},\d$", t["text"])]
    candidato = min(numeros, key=lambda t: abs(t["cy"] - sudeste["cy"]) + abs(t["cx"] - col2025["cx"]))

    num_str = candidato["text"].replace(".", "").replace(",", ".")
    try:
        return float(num_str)
    except:
        return num_str


valor = extrair_numero(IMG_PATH)
print(valor)
