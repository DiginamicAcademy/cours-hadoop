#!/usr/bin/env python3
"""qualite.py — vérifie qu'un markdown de cours est propre.

Usage : qualite.py <fichier|dossier>
Applique la checklist references/analyses/checklist-markdown.md.
Sortie : un tableau markdown (règle · fichier · ligne · sévérité) sur stdout,
et le code de sortie 1 s'il existe au moins une erreur bloquante.
Sévérités : error (bloque) / warn (documente). Les blocs de code sont ignorés
(hormis la recherche de secrets), le rapport est plafonné à 8 constats par
règle et par fichier.
"""
import pathlib
import re
import sys
from collections import defaultdict

# Règle 6.3 — éléments de conception interdits dans un dépôt PUBLIC. La
# correspondance se fait sur les JETONS des composants du chemin (découpés sur
# tout caractère non alphanumérique), jamais en sous-chaîne : en sous-chaîne,
# « 02-corriger-injection.md » (un TP sur la correction d'injection) passait
# pour un corrigé à cause du verbe « corriger », et « 06-cadre-juridique-intro »
# pour un ADR à cause de « c-adr-e ».
# « corrige » / « corriges » ne figurent PAS dans la liste : décision Robin du
# 04/11/2026 — les corrigés FORMATIFS restent publics (tp/corrige/ du dépôt de
# cours), seuls les 3 corrigés de TP notés vivent au -Peda. Un motif « corrige »
# marquerait donc 42 fichiers légitimes. La fuite d'un corrigé NOTÉ dans le
# public ne peut pas se détecter par le chemin : elle relève de la relecture.
INTERDITS_PUBLIC = ("cadrage", "cor", "adr", "progress")
# Dépôt de conception privé (<cours>-Peda, ou le conteneur peda/ du super-repo) :
# 6.3 n'y a pas lieu d'être — cadrage/, PROGRESS.md et corrigés y sont chez eux.
# Un dépôt privé cloné sous un nom sans ce marqueur ne serait pas reconnu : le
# passer avec son nom réel (le chemin est résolu en absolu, le nom du dépôt y est).
MARQUEUR_PRIVE = re.compile(r"(?:^|[-_])peda$")
SECRETS = (r"ghp_[A-Za-z0-9]{20,}", r"github_pat_[A-Za-z0-9_]{20,}", r"AKIA[0-9A-Z]{16}",
           r"BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY")
BLOOM = re.compile(r"\[\s*Bloom\s*\d", re.IGNORECASE)
ACRONYME = re.compile(r"\b([A-ZÀ-Ý]{3,}s?)\b")
ROMAIN = re.compile(r"M{0,3}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})")
ACRONYMES_OK = {"TODO", "FIXME", "UTF", "HTML", "YAML", "JSON", "PDF", "PPTX", "API", "CI",
                "RGPD", "RGAA", "SQL", "CSS", "HTTP", "HTTPS", "URL", "TP", "TD", "QCM", "VPN",
                # mots-clés de callouts Obsidian (`> [!TIP]` …) — pas des acronymes
                "TIP", "WARNING", "DANGER", "INFO", "NOTE", "IMPORTANT", "CAUTION", "SUCCESS", "QUESTION", "QUOTE", "EXAMPLE", "ABSTRACT", "BUG", "FAILURE", "SUMMARY"}

resultats = []
compteurs = defaultdict(int)


def noter(regle, fichier, ligne, severite, message):
    compteurs[(regle, str(fichier))] += 1
    if compteurs[(regle, str(fichier))] <= 8:
        resultats.append((regle, str(fichier), ligne, severite, message))


def marquer_code(lignes):
    """Retourne la liste des booléens « cette ligne est dans un bloc de code »."""
    dans, flags = False, []
    for l in lignes:
        flags.append(dans)
        if l.strip().startswith("```"):
            dans = not dans
    return flags


def check_structure(path, lignes, code):
    h1 = [(i + 1, l) for i, l in enumerate(lignes) if not code[i] and re.match(r"^# ", l)]
    # Règle 1.1 non applicable à un diaporama Marp : chaque diapositive y est
    # un H1 par construction, comme l'admet déjà la règle 1.4 ligne 83 (parti
    # pris Yuto du 25/11/2026, dans l'esprit de la correction de 6.3 du 06/11).
    # 1.2 et 1.3 continuent de courir sur slides.md.
    if path.name != "slides.md":
        for i, l in h1[1:]:
            noter("1.1", path, i, "error", f"plusieurs H1 — « {l[2:60]} »")
    niveaux, titres = [], {}
    for i, l in enumerate(lignes):
        if code[i]:
            continue
        m = re.match(r"^(#{1,6}) ", l)
        if not m:
            continue
        n = len(m.group(1))
        if niveaux and n > niveaux[-1] + 1:
            noter("1.2", path, i + 1, "error", f"saut de niveau H{niveaux[-1]}→H{n}")
        niveaux.append(n)
        cle = l.strip().lower()
        if cle in titres:
            noter("1.3", path, i + 1, "warn", f"titre dupliqué (ligne {titres[cle]})")
        titres[cle] = i + 1
    if path.name not in ("Readme.md", "README.md", "readme.md", "slides.md", "config.yaml"):
        if not re.match(r"^([0-9]+|[A-Za-z])[-._ ]", path.name):
            noter("1.4", path, 0, "warn", "nom de fichier sans préfixe chapitre (01-… / A-…)")


def check_markdown(path, lignes, code):
    for i, l in enumerate(lignes):
        if code[i]:
            continue
        if l.strip().startswith("```"):
            continue
        if l != l.rstrip():
            noter("2.1", path, i + 1, "warn", "espace(s) en fin de ligne")
        # doubles espaces en milieu de ligne uniquement (l'indentation et la
        # coupure de ligne markdown « deux espaces en fin » sont légitimes)
        corps = l.rstrip()
        if corps == corps.lstrip() and "  " in corps.strip():
            noter("2.1", path, i + 1, "warn", "doubles espaces en milieu de ligne")
        if re.search(r"<details", l, re.IGNORECASE) and not any(
            re.search(r"</details>", x, re.IGNORECASE) for x in lignes[i:]):
            noter("2.4", path, i + 1, "error", "<details> jamais fermé")
    if code[-1] if code else False:
        noter("2.2", path, len(lignes), "error", "bloc de code non fermé")


def check_liens_images(path, lignes, code, racine):
    for i, l in enumerate(lignes):
        if code[i]:
            continue
        for m in re.finditer(r"\[([^\]]*)\]\(([^)]+)\)", l):
            texte, cible = m.group(1), m.group(2).split("#")[0]
            if not texte.strip() or texte.strip().lower() in ("cliquez ici", "ici", "click here"):
                noter("3.4", path, i + 1, "warn", f"lien sans nom de cible « {texte} »")
            if not cible or cible.startswith(("http://", "https://", "mailto:")):
                continue
            if re.match(r"^[A-Za-z]:\\|^/home/|^/Users/", cible):
                noter("3.3", path, i + 1, "error", f"chemin absolu « {cible} »")
                continue
            if not (path.parent / cible).exists() and not (racine / cible).exists():
                noter("3.1", path, i + 1, "error", f"lien cassé « {cible} »")
        for m in re.finditer(r"!\[([^\]]*)\]\(([^)]+)\)", l):
            alt, cible = m.group(1), m.group(2)
            if not alt.strip():
                noter("3.2", path, i + 1, "warn", "image sans texte alternatif")
            if not cible.startswith(("http://", "https://")) and not (path.parent / cible).exists():
                noter("3.2", path, i + 1, "error", f"image introuvable « {cible} »")


def check_francais(path, lignes, code):
    for i, l in enumerate(lignes):
        if code[i] or l.strip().startswith("|"):
            continue
        # une espace ORDINAIRE (U+0020) juste avant : ; ? ! — l'insécable est correcte
        if re.search(r" [:;!?]", l):
            noter("4.1", path, i + 1, "warn", "espace insécable manquante avant : ; ? !")
        if "«" in l and re.search(r"«[^»\s]", l):
            noter("4.1", path, i + 1, "warn", "espace insécable manquante après «")
        if "»" in l and re.search(r"[^«\s]»", l):
            noter("4.1", path, i + 1, "warn", "espace insécable manquante avant »")
        if '"' in l and ("«" in l or "»" in l):
            noter("4.3", path, i + 1, "warn", "guillemets « » et \"\" mélangés")


def check_contenu(path, lignes, code):
    # une définition accepte un sigle en gras : « **ANSSI** (Agence nationale…) »
    definis = set(re.findall(r"\b([A-ZÀ-Ý]{3,}s?)\b\**\s*\(", "\n".join(lignes)))
    vue = set()
    for i, l in enumerate(lignes):
        if BLOOM.search(l):
            noter("5.1", path, i + 1, "error", "niveau Bloom visible côté apprenant")
        for motif in SECRETS:
            if re.search(motif, l):
                noter("6.1", path, i + 1, "error", "secret potentiel en clair")
                break
        if code[i]:
            continue
        for m in ACRONYME.finditer(l):
            sigle = m.group(1)
            if ROMAIN.fullmatch(sigle):
                continue
            if sigle in vue or sigle in definis or sigle in ACRONYMES_OK:
                continue
            vue.add(sigle)
            noter("4.5", path, i + 1, "warn", f"acronyme « {sigle} » jamais défini")


def check_public(path):
    """Règle 6.3 — rien de la conception dans un dépôt public.

    Un seul constat par fichier (le premier motif rencontré), comme pour les
    secrets en 6.1 : auparavant « adr » recouvrait « cadrage » et chaque fichier
    de conception était signalé deux fois pour le même motif.
    """
    dossiers = [c.lower() for c in path.parts[:-1]]
    if any(MARQUEUR_PRIVE.search(c) for c in dossiers):
        return
    jetons_dossiers = set()
    for composant in dossiers:
        jetons_dossiers.update(re.split(r"[^a-z0-9]+", composant))
    jetons_fichier = set(re.split(r"[^a-z0-9]+", path.stem.lower()))
    for interdit in INTERDITS_PUBLIC:
        if interdit in jetons_dossiers:
            noter("6.3", path, 0, "error", f"élément de conception interdit dans le dépôt public ({interdit}/)")
            return
        if interdit in jetons_fichier:
            noter("6.3", path, 0, "error", f"élément de conception interdit dans le dépôt public ({interdit})")
            return


def main():
    if len(sys.argv) < 2:
        print(__doc__.strip())
        sys.exit(2)
    cible = pathlib.Path(sys.argv[1]).resolve()
    racine = cible if cible.is_dir() else cible.parent
    fichiers = sorted(cible.rglob("*.md")) if cible.is_dir() else [cible]
    fichiers = [f for f in fichiers if ".git" not in f.parts]

    for f in fichiers:
        lignes = f.read_text(encoding="utf-8").splitlines()
        code = marquer_code(lignes)
        check_public(f)
        check_structure(f, lignes, code)
        check_markdown(f, lignes, code)
        check_liens_images(f, lignes, code, racine)
        check_francais(f, lignes, code)
        check_contenu(f, lignes, code)

    erreurs = [r for r in resultats if r[3] == "error"]
    print("| Règle | Fichier | Ligne | Sévérité | Message |")
    print("|---|---|---|---|---|")
    for regle, fichier, ligne, sev, msg in sorted(resultats):
        print(f"| {regle} | {fichier} | {ligne or '—'} | {sev} | {msg} |")
    masques = sum(v for v in compteurs.values()) - len(resultats)
    if masques:
        print(f"\n({masques} constat(s) supplémentaire(s) masqué(s) — plafond de 8 par règle et par fichier.)")
    print(f"\n{len(fichiers)} fichier(s) vérifié(s) — {len(erreurs)} erreur(s), {len(resultats) - len(erreurs)} avertissement(s).")
    sys.exit(1 if erreurs else 0)


main()
