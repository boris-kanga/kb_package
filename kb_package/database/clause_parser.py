import re


class ClauseParser:

    @staticmethod
    def get_structures(str_statement):
        str_statement = str_statement.strip()
        opened_bracket_number = 0
        cl = re.compile(r"\s(and|or)\s", flags=re.S | re.I)
        consider_text = ""
        calculation = []
        for char in str_statement:
            if char == "(":
                if opened_bracket_number == 0:
                    # print(consider_text)
                    consider_text = consider_text.strip()
                    if consider_text:
                        calculation += [p.strip() for p in cl.split(" "+consider_text+" ") if len(p.strip())]
                        # calculation.append(consider_text)
                    # print(cl.findall(consider_text))
                    consider_text = ""
                else:
                    consider_text += "("
                opened_bracket_number += 1
            elif char == ")":
                opened_bracket_number -= 1
                if opened_bracket_number == 0:
                    # print(consider_text)
                    calculation.append(ClauseParser.get_structures(consider_text))
                    consider_text = ""
                else:
                    consider_text += ")"

            else:
                consider_text += char
        if consider_text.strip():
            calculation += [p.strip() for p in cl.split(" " + consider_text + " ") if len(p.strip())]
            # calculation.append(consider_text.strip())
        return calculation

    @staticmethod
    def render(str_statement):
        struc = ClauseParser.get_structures(str_statement)
        print(struc)
        for part in struc:
            if isinstance(part, str):
                g = re.match(
                    r"(.*)\s*(\s+in\s+|>=|<=|<|>|!=)\s*(.*)",
                    part, flags=re.I | re.S)

                if g is not None:
                    g = g.groups()
                    print(g)
                    var, op, val = g
                    op = op.strip()


if __name__ == '__main__':
    print(ClauseParser.render("HHH>=3 and (HP=3 or H=3) or (hpp=3 or (a='HHH' or y='22')) and m=3"))
