from difflib import SequenceMatcher

def tokenize(text):
    return text.split()

def get_token_diff(prev_tokens, curr_tokens):
    sm = SequenceMatcher(None, prev_tokens, curr_tokens)
    added = []
    removed = []

    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "insert":
            added.extend(curr_tokens[j1:j2])
        elif tag == "delete":
            removed.extend(prev_tokens[i1:i2])
        elif tag == "replace":
            removed.extend(prev_tokens[i1:i2])
            added.extend(curr_tokens[j1:j2])

    return added, removed


if __name__ == "__main__":
    prev = "Wikipedia Tiếng Việt Xin chào! Mình muốn tìm người cùng xây dựng encyclopedia Tiếng Việt ở đây, có ai muốn hợp tác không? Xin liên hệ Vieilletortue@yahoo.com nhé ! Mình không biết Tiếng Việt giỏi lắm, mà mình muốn giúp ở đây một tí. – Minh Nguyễn Rùa. Come on Vietnamese speaking people! (And anyone who knows just a little Vietnamese!) Vietnamese language. If you speak this language and think it would be cool to have your own Encyclopedia then you can make it. Go ahead Delete this and start working on your Encyclopedia. Muốn biết nữa thì xin đi website chính. IS IT A GOOD IDEA TO TRY TO CONVERT IT INTO WORKING WITH UNICODE? I have asked the Wikipedia people if it is possible to move it to http://vi.wikipedia.org in order for us to use Unicode (UTF-8) Internet Society Việt Nam: ISOC Thành Phố Hà nội Sài Gòn Nha Trang Tiến Quân Ca, phố Lý Thường Kiệt GỢI Ý XÂY DỰNG WEBSITE HỖ TRỢ NGƯỜI TÀN TẬT Tiếng Khác Afrikaans - (Araby) - Armena - Aymara - Azeri - Bahasa Melayu - (Balgarski) - Bosanski - Brezhoneg - Català - Česky - Corsu - Cymraeg - Dansk - Deutsch - Eesti - Ελληνικά (Ellenika) - English - Esperanto - Euskara - Farsi - Suomeksi - Français - Frysk - Gaelige - Galego - Guaraní - Gujarati - (Ivrit) - Hindi - Hrvatski - Interlingua - Italiano - (Nihongo) - Latina - Latviešu - Lietuųu - Magyar - (Makedonska) - Malayalam - Marathi - Nahuatl - Nederlands - Norsk - Occitan - Platdüütsch - (Poe-Skey) (Tibetan) - Polska - Punjabi - Quechua - Rom. - (Russkiy) - Shqiptare - Slovenčina - Slovensko - (Srpski) - Svenska - Swahili - Tatarça - (Thai) - Türkçe - (Ukrains'ka) - Volapük - (Zhongwen) Của Wikimedia September 11 Memorial Wiki - Meta-Wikipedia - Wiktionary - Wikibooks - Wikiquote - WikiSource".split()
    curr = "Wikipedia Tiếng Việt Xin chào! Mình muốn tìm người cùng xây dựng encyclopedia Tiếng Việt ở đây, có ai muốn hợp tác không? Xin liên hệ Vieilletortue@yahoo.com nhé ! Mình không biết Tiếng Việt giỏi lắm, mà mình muốn giúp ở đây một tí. – Minh Nguyễn Rùa. Come on Vietnamese speaking people! (And anyone who knows just a little Vietnamese!) Help create the Vietnamese-Language Wikipedia! If you speak this language and think it would be cool to have your own Encyclopedia then you can make it. Go ahead Delete this and start working on your Encyclopedia. Muốn biết nữa thì xin đi website chính. IS IT A GOOD IDEA TO TRY TO CONVERT IT INTO WORKING WITH UNICODE? I have asked the Wikipedia people if it is possible to move it to http://vi.wikipedia.org in order for us to use Unicode (UTF-8) Internet Society Việt Nam: ISOC Thành Phố Hà nội Sài Gòn Nha Trang Tiến Quân Ca, phố Lý Thường Kiệt GỢI Ý XÂY DỰNG WEBSITE HỖ TRỢ NGƯỜI TÀN TẬT Tiếng Khác Afrikaans - (Araby) - Armena - Aymara - Azeri - Bahasa Melayu - (Balgarski) - Bosanski - Brezhoneg - Català - Česky - Corsu - Cymraeg - Dansk - Deutsch - Eesti - Ελληνικά (Ellenika) - English - Esperanto - Euskara - Farsi - Suomeksi - Français - Frysk - Gaelige - Galego - Guaraní - Gujarati - (Ivrit) - Hindi - Hrvatski - Interlingua - Italiano - (Nihongo) - Latina - Latviešu - Lietuųu - Magyar - (Makedonska) - Malayalam - Marathi - Nahuatl - Nederlands - Norsk - Occitan - Platdüütsch - (Poe-Skey) (Tibetan) - Polska - Punjabi - Quechua - Rom. - (Russkiy) - Shqiptare - Slovenčina - Slovensko - (Srpski) - Svenska - Swahili - Tatarça - (Thai) - Türkçe - (Ukrains'ka) - Volapük - (Zhongwen) Của Wikimedia September 11 Memorial Wiki - Meta-Wikipedia - Wiktionary - Wikibooks - Wikiquote - WikiSource".split()

    added, removed = get_token_diff(prev, curr)
    print("Added:", added)
    print("Removed:", removed)