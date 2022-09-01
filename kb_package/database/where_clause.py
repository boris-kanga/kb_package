# -*- coding: utf-8 -*-
import datetime
import random
import re
import string


class WhereClause:
    EQUAL = "="
    DIFFERENT = "!="
    BETWEEN = 3
    IN = "in"
    IS = "is"
    SUPERIOR = ">"
    INFERIOR = "<"
    SUPERIOR_E = ">="
    INFERIOR_E = "<="
    LIKE = "like"
    DATE_TIME_FORMAT = "(\d{4})-(\d{1,2})-(\d{1,2})( " \
                       "(\d{1,2}):(\d{1,2}):(\d{1,2}))?"

    MONGO_WHERE_CLAUSE_EQ = {
        DIFFERENT: "$neq",
        SUPERIOR: "$gt",
        INFERIOR: "$lt",
        SUPERIOR_E: "$gte",
        INFERIOR_E: "$lte",
        IN: "$in"
    }

    @staticmethod
    def replacer_treatment(str_statement, additional_character,
                           replace_value=None,
                           args="'"):
        if replace_value is None:
            replace_value = ["\\(", '\\)']
        if isinstance(replace_value, str):
            replace_value = [replace_value]
        for r in replace_value:
            res = re.findall(rf"({args}.*{r}.*{args})", str_statement, flags=re.S)
            for i in res:
                replace_value = i.replace(r, r + additional_character)
                str_statement = str_statement.replace(i, replace_value)
        return str_statement

    @staticmethod
    def text_treatment(str_statement, replace_value=None,
                       final_character=None):
        if final_character is None:
            final_character = WhereClause.get_first_characters_no_use(
                str_statement)
        for car in "'\"`":
            str_statement = WhereClause.replacer_treatment(str_statement,
                                                           final_character,
                                                           replace_value,
                                                           car)

        return str_statement, final_character

    @staticmethod
    def get_first_characters_no_use(str_statement, no_want=")("):
        extra = string.punctuation
        if no_want is not None:
            for i in no_want:
                extra = extra.replace(i, "")
        extra = [e for e in extra]
        while True:
            for i in range(1, len(extra) - 1):
                for e in [
                    extra[start: start + i]
                    for start in range(0, len(extra) - i, i)
                ]:
                    e = "".join(e)
                    if e not in str_statement:
                        final_character = ""
                        for car in e:
                            if car in ".^$*+-?()[]{}\\|—/<>=":
                                car = "\\" + car
                            final_character += car
                        return final_character
            random.shuffle(extra)

    @staticmethod
    def get_structures(str_statement, final_character=None):
        str_statement, final_character = WhereClause.text_treatment(
            str_statement.strip(), final_character
        )
        if str(str_statement).startswith("("):
            str_statement = "1 AND " + str_statement
        print(str_statement)
        print(final_character)
        regex = re.compile(
            rf"(\s(?:and|or)\s+\((?!{final_character}))", flags=re.S | re.I
        )
        structures = regex.split(str_statement)
        # print("struc==", len(structures))
        last_structure_pattern_start = re.compile(
            r"^\s(and|or)\s+\($", flags=re.S | re.I
        )
        last_structure_pattern_end = None
        current_working_text = None
        number_of_clone = 0
        my_strucs = []
        and_or = None
        print('#', structures)
        for s in structures:
            print("@", s)
            s_start = last_structure_pattern_start.match(s)
            if last_structure_pattern_end is not None:
                # print("----"*10)
                s_end = re.findall(
                    rf"(.*?)\)(?!{final_character})\s+(?:or|and)",
                    s, flags=re.S | re.I)
                if len(s_end):
                    print("------------end---------")
                    pass
                else:
                    # s_end = last_structure_pattern_end.findall(s)
                    if len(s.rstrip()) and s.rstrip()[-1] == ")":
                        s_end = [s.rstrip()[:-1]]
                    else:
                        s_end = False
                # print("end",s_end)
                # print("----"*10)

            else:
                s_end = False
            if s_start and current_working_text is None:
                # print("new-para")
                current_working_text = ""
                and_or = s_start.groups()[0]
                last_structure_pattern_end = re.compile(
                    rf"(.*?)\)(?!{final_character})", flags=re.S | re.I
                )
                number_of_clone = 0
            elif s_end and current_working_text is not None:
                # print("here", s_end, current_working_text)
                add_code = ""
                # print("----s_end", s_end)
                for index, end in enumerate(s_end):
                    add_code += end
                    number_of_clone -= 1
                    if number_of_clone <= 0:
                        break
                    else:
                        add_code += ")"
                # print("code", add_code, number_of_clone)
                if number_of_clone <= 0:
                    current_working_text += add_code

                    add_code = ''.join(
                        s.split(current_working_text, 1)[1:])[1:]

                    # print("add",add_code)

                    current_working_text = current_working_text.replace(
                        "(" + final_character, "("
                    )
                    current_working_text = current_working_text.replace(
                        ")" + final_character, ")"
                    )

                    my_strucs.append(
                        {
                            "conjunction": and_or,
                            "clause": WhereClause.get_structures(
                                current_working_text, final_character
                            ),
                        }
                    )
                    current_working_text = None
                    last_structure_pattern_end = None
                    if len(add_code.strip()):
                        s = add_code.strip()
                        # print(s)
                        try:
                            and_or, where = re.search("\s*(and|or)\s(.*)$", s,
                                                      flags=re.I | re.S
                                                      ).groups()
                            # print(where)

                            where = where.replace("(" + final_character, "(")
                            where = where.replace(")" + final_character, ")")

                            where = WhereClause.get_unit_clause(where)

                            first, others = where[0], where[1:]
                            my_strucs.append({
                                "conjunction": and_or,
                                "clause": first
                            })
                            
                            my_strucs.extend(others)
                            
                        except AttributeError:
                            pass
                else:
                    current_working_text += s

            else:
                if s_start and current_working_text is not None:
                    # print("got -para")
                    number_of_clone += 1
                if current_working_text is None:
                    s = s.replace("(" + final_character, "(")
                    s = s.replace(")" + final_character, ")")
                    my_strucs.append(s)
                else:
                    # print("add--",s)
                    current_working_text += s

        print(my_strucs)
        return my_strucs

    @staticmethod
    def get_unit_clause(str_statement, final_character=None):

        str_statement, final_character = \
            WhereClause.text_treatment(str_statement, ["or", "and"],
                                       final_character)
        my_strucs = []
        part = re.split(r"\s(or|and)\s+", str_statement, flags=re.I | re.S)
        my_strucs.append(part[0])
        for op, where in zip(part[1::2], part[2::2]):
            where = where.replace("and" + final_character, "and")
            where = where.replace("or" + final_character, "or")

            my_strucs.append({
                "conjunction": op or "and",
                "clause": where
            })
        return my_strucs

    @staticmethod
    def parse_value_to_mongo(val: str = "NULL"):
        val = val.strip()
        if re.match(WhereClause.DATE_TIME_FORMAT, val):
            data = [int(d) or 0 for d in re.match(WhereClause.DATE_TIME_FORMAT,
                                                  val).groups()]
            val = datetime.datetime(*data)
        else:
            if val.lower() == "null":
                val = None
            else:
                try:
                    val = eval(val)
                except (SyntaxError, NameError):
                    pass
        return val

    @staticmethod
    def parse_where_clause_to_mongo(str_statement):
        str_statement = str_statement.strip()
        my_strucs = WhereClause.get_structures(str_statement)
        where = {}
        # print(my_strucs)
        for statement in my_strucs:
            if isinstance(statement, str):
                statement = statement.strip()
                if statement.lower() in ["1", "true", ""]:
                    pass
                else:
                    statement = WhereClause.get_unit_clause(statement)
                    for w in statement:
                        if isinstance(w, str):
                            w = {"clause": w, "conjunction": "and"}
                        conj, clause = w["conjunction"], w["clause"]
                        clause = clause.strip()
                        if clause.lower() in ["1", "true", ""]:
                            continue
                        else:
                            g = re.match(
                                "(.*)\s*(\s+in\s+|>=|<=|<|>|!=)\s*(.*)",
                                clause, flags=re.I | re.S)
                            if g is not None:
                                var, op, val = g.groups()
                                op = op.strip()

                                clause = {
                                    WhereClause.MONGO_WHERE_CLAUSE_EQ[
                                        op]: WhereClause.parse_value_to_mongo(
                                        val)
                                }
                            else:
                                g = re.match(
                                    "(.*)\s*(=|\s+is\s+)\s*(.*)",
                                    clause, flags=re.I | re.S)
                                if g is not None:
                                    var, op, val = g.groups()
                                    op = op.strip()
                                    if op in ["=", "is"]:
                                        clause = \
                                            WhereClause.parse_value_to_mongo(
                                                val)
                                else:
                                    continue
                        if conj == "or":
                            where["$or"] = where.get("$or", [])
                            where["$or"].append({var: clause})
                        else:
                            where[var] = clause
            else:
                and_or = statement["conjunction"] or "and"
                where["$" + and_or] = where.get("$" + and_or, [])
                if isinstance(statement["clause"], str):
                    statement["clause"] = [statement["clause"]]
                for w in statement["clause"]:
                    add_where = where["$" + and_or]
                    if isinstance(w, str):
                        w = WhereClause.parse_where_clause_to_mongo(w)
                        add_where.append(w)

                    elif isinstance(w, dict):
                        if w["conjunction"] == "or":
                            add_where = [p for p in add_where
                                         if list(p.keys())[0] == "$or"]
                            if not len(add_where):
                                where["$" + and_or].append({"$or": []})
                                add_where = where["$" + and_or][-1]
                            else:
                                add_where = add_where[0]
                            add_where = add_where['$or']
                        for clause in w["clause"]:
                            clause = WhereClause.parse_where_clause_to_mongo(
                                clause)

                            add_where.append(clause)

        return where


if __name__ == "__main__":
    print(WhereClause.parse_where_clause_to_mongo("HHH>=3 and (HP=3 or H=3) or (hpp=3 or (a='HHH' or y='22')) and m=3"))
    if False:
        print(
            WhereClause.parse_where_clause_to_mongo("""
                 
                souscription.end_date >= 2
                            AND purpose.activated = "Yes"
                            AND (souscription.date_resil is NULL)
                            AND souscription.booking = 1
                            AND souscription.booked = 0
                            AND souscription.processing = 1
                            AND purpose.id_purpose = 156
                            
                            AND purpose.id_purpose>0
                            AND souscription.email='vgryner@gmail.com'
                """)
        )
