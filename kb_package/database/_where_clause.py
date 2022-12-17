import ast
import re
from kb_package import tools


class Where(ast.NodeTransformer):
    FUNC_EQ = {
        "lower": "$toLower",
        "upper": "$toUpper"
    }
    MONGO_WHERE_CLAUSE_EQ = {
        ast.Eq: "$eq",
        ast.NotEq: "$neq",
        # ast.Not: "$not",
        # ast.BitXor: "$nor",
        ast.Gt: "$gt",
        ast.Lt: "$lt",
        ast.GtE: "$gte",
        ast.LtE: "$lte",
        ast.In: "$in",
        ast.NotIn: "$nin"
        # Is
    }

    def visit_Compare(self, node):
        node.left = self.visit(node.left)
        comparators = []
        for val in node.comparators:
            val = self.visit(val)
            comparators.append(val)
        node.comparators = comparators

        if isinstance(node.ops[0], (ast.Is, ast.IsNot)):
            if isinstance(node.comparators[0], ast.Tuple):
                # need to be 2 size -> (between _min and _max)
                _min, _max = node.comparators[0].elts

                result = ast.BoolOp(ast.And(), [
                    ast.Compare(node.left, [ast.GtE()], [_min]),
                    ast.Compare(node.left, [ast.LtE()], [_max])
                ])

                if isinstance(node.ops[0], ast.IsNot):
                    result = ast.UnaryOp(ast.Not(), result)
                result = self.visit(result)
                return ast.copy_location(result, node)
            elif isinstance(node.comparators[0], ast.Str):
                # field is "ksksjjsjsj" -> field like "djdjdj"
                result = ast.Call(ast.Name("like", ast.Load()),
                                  [node.left,
                                   node.comparators[0],
                                   ast.Constant(isinstance(node.ops[0], ast.IsNot))
                                   ], []
                                  )
                return ast.copy_location(result, node)
            else:
                if node.comparators[0].id.lower() in ("null", "none"):
                    result = ast.Compare(node.left,
                                         [ast.Eq() if isinstance(node.ops[0], ast.IsNot) else ast.NotEq()],
                                         [node.left])

                    return ast.copy_location(result, node)

        if isinstance(node.ops[0], (ast.In, ast.NotIn)):
            # print(ast.dump(node, indent=4))
            result = ast.Call(ast.Name("in_func", ast.Load()),
                              [node.left,
                               node.comparators[0],
                               ast.Constant(isinstance(node.ops[0], ast.NotIn))
                               ], []
                              )
            return ast.copy_location(result, node)
        return node

    @staticmethod
    def parse_where_clause_to_mongo(query):
        query, quoted_text_dict = tools.replace_quoted_text(query)
        # replace = by ==
        query = re.sub(r"(?<!=)=(?!=)", "==", query, flags=re.I)
        # replace <> by !=
        query = query.replace("<>", "!=")

        # extra -> like and not like
        query = re.sub(r"\s+not\s+like\s+", " is not ", query, flags=re.I)
        query = re.sub(r"\s+like\s+", " is ", query, flags=re.I)
        # between
        query = re.sub(r"\s+(not\s+)?between\s+(\w+)\s+and\s+(\w+)", r" is \1 (\2, \3) ", query, flags=re.I)

        # keyword case
        for keyword in ["in", "is", "and", "or", "not"]:
            query = re.sub(keyword, keyword, query, flags=re.I)
        # consider the query where not hard
        old_query = query

        # for hard queries -- modifying of query
        # query = re.sub(r"\s+or\s+", " | ", query, flags=re.I)
        # query = re.sub(r"\s+and\s+", " & ", query, flags=re.I)

        for k in quoted_text_dict:
            query = query.replace(k, quoted_text_dict[k])
            old_query = old_query.replace(k, quoted_text_dict[k])
        node = ast.parse(query)
        if not all([isinstance(n, ast.Expr) for n in node.body]):
            raise ValueError("Bad value given")
        # node = Where().visit(node)
        print(ast.unparse(node))
        print(ast.dump(node, indent=4))
        # {'HHH': {'$gte': 3}, '$and': [{'HP': 3, '$or': [{'H': 3}]}],
        # '$or': [{'hpp': 3}, {'$or': [{"a='HHH' or! y": '22'}]}]}
        return Where.get_mongo_where_clause_from_node(node.body[0].value)

    @staticmethod
    def get_mongo_where_clause_from_node(node):

        if isinstance(node, ast.BoolOp):
            result = {}
            op = ""
            if isinstance(node.op, ast.And):
                op = "$and"
            elif isinstance(node.op, ast.Or):
                op = "$or"
            result[op] = []
            for nn in node.values:
                result[op].append(Where.get_mongo_where_clause_from_node(nn))
            return result
        if isinstance(node, ast.Compare):
            if isinstance(node.ops[0], (ast.Is, ast.IsNot)):
                # consider: like, between, is null
                if isinstance(node.comparators[0], ast.Tuple):
                    # between
                    _min, _max = node.comparators[0].elts
                    return {
                        Where.get_node_value(node.left, add=""): {
                            "$lte": _max.value, "$gte": _min.value
                        }
                    }
                elif isinstance(node.comparators[0], ast.Str):
                    return {
                        Where.get_node_value(node.left, add=""): {
                            "$regex": node.comparators[0].value
                        }
                    }
                else:
                    if node.comparators[0].id.lower() in ("null", "none"):
                        return {
                            Where.get_node_value(node.left, add=""): None
                        }
                    raise ValueError("Bad where clause gotten")
            else:
                op = node.ops[0]
                return {
                    Where.get_node_value(node.left, add=""): {
                        Where.MONGO_WHERE_CLAUSE_EQ.get(type(op)): Where.get_node_value(node.comparators[0])
                    }
                }
        if isinstance(node, ast.Call):
            if node.func.id.lower() in Where.FUNC_EQ:
                pass
        if isinstance(node, ast.UnaryOp):
            # unaryOp are (not ~)
            if isinstance(node.op, ast.Not):
                return {"$not": Where.get_mongo_where_clause_from_node(node.operand)}
        if isinstance(node, ast.Name):
            # return bool(node)
            pass
        return {}

    @staticmethod
    def get_node_value(node, add="$"):
        if isinstance(node, ast.Constant):
            right = node.value
        elif isinstance(node, ast.Name):
            right = add + node.id
        elif isinstance(node, ast.Call):
            right = ""
        else:
            right = ""
        return right


if __name__ == '__main__':
    print(Where.parse_where_clause_to_mongo("not (b=3 or a between 100 and 300) and c >0"))
