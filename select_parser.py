# select_parser.py
# Copyright 2010, Paul McGuire
#
# a simple SELECT statement parser, taken from SQLite's SELECT statement
# definition at http://www.sqlite.org/lang_select.html
#
from pyparsing import *
import sys

LPAR,RPAR,COMMA = map(Suppress,"(),")
select_stmt = Forward().setName("select statement")

# keywords
(COUNT, UNION, ALL, AND, OR, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER,
 CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY,
 HAVING, ORDER, BY, LIMIT, OFFSET) =  map(CaselessKeyword, """COUNT, UNION, ALL, AND, OR, INTERSECT,
 EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT,
 DISTINCT, FROM, WHERE, GROUP, BY, HAVING, ORDER, BY, LIMIT, OFFSET""".replace(",","").split())
(CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
 COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE,
 CURRENT_TIMESTAMP) = map(CaselessKeyword, """CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE,
 END, CASE, WHEN, THEN, EXISTS, COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE,
 CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP""".replace(",","").split())
keyword = MatchFirst((COUNT, UNION, ALL, OR, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER,
 CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY,
 HAVING, ORDER, BY, LIMIT, OFFSET, CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
 COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE,
 CURRENT_TIMESTAMP))

identifier = ~keyword + Word(alphas, alphanums+"_")
collation_name = identifier.copy()
column_name = identifier.copy()
column_alias = identifier.copy()
table_name = identifier.copy()
table_alias = identifier.copy()
index_name = identifier.copy()
function_name = identifier.copy()
parameter_name = identifier.copy()
database_name = identifier.copy()

# expression
expr = Forward().setName("expression")

integer = Regex(r"[+-]?\d+")
numeric_literal = Regex(r"\d+(\.\d*)?([eE][+-]?\d+)?")
string_literal = QuotedString("'")
blob_literal = Combine(oneOf("x X") + "'" + Word(hexnums) + "'")
literal_value = ( numeric_literal | string_literal | blob_literal |
    NULL | NOT + NULL | CURRENT_TIME | CURRENT_DATE | CURRENT_TIMESTAMP )
bind_parameter = (
    Word("?",nums) |
    Combine(oneOf(": @ $") + parameter_name)
    )
type_name = oneOf("TEXT REAL INTEGER BLOB NULL")

val_list = LPAR + delimitedList(literal_value) + RPAR

count_term = COUNT + LPAR + ('*' | identifier) + RPAR
expr_term = (
    CAST + LPAR + expr + AS + type_name + RPAR |
    EXISTS + LPAR + select_stmt + RPAR |
    count_term('count') |
    function_name + LPAR + Optional(delimitedList(expr)) + RPAR |
    literal_value |
    bind_parameter |
    identifier |
    val_list |
    expr
    )

UNARY,BINARY,TERNARY=1,2,3
expr << operatorPrecedence(expr_term,
    [
    (oneOf('- + ~') | NOT, UNARY, opAssoc.LEFT),
    ('||', BINARY, opAssoc.LEFT),
    (oneOf('* / %'), BINARY, opAssoc.LEFT),
    (oneOf('+ -'), BINARY, opAssoc.LEFT),
    (oneOf('<< >> & |'), BINARY, opAssoc.LEFT),
    (oneOf('< <= > >='), BINARY, opAssoc.LEFT),
    (oneOf('= == != <>') | IS | IN | LIKE | GLOB | MATCH | REGEXP, BINARY, opAssoc.LEFT),
    ((AND | OR), BINARY, opAssoc.LEFT),
    ('||', BINARY, opAssoc.LEFT),
    ((BETWEEN,AND), TERNARY, opAssoc.LEFT),
    ])


compound_operator = (UNION + Optional(ALL) | INTERSECT | EXCEPT)

ordering_term = identifier | delimitedList(identifier) | expr + Optional(COLLATE + collation_name) + Optional(ASC | DESC)

join_constraint = Optional(ON + expr | USING + LPAR + Group(delimitedList(column_name)) + RPAR)

join_op = COMMA | (Optional(NATURAL) + Optional(INNER | CROSS | LEFT + OUTER | LEFT | OUTER) + JOIN)

join_source = Forward()
single_source = ( (Group(database_name("database") + "." + table_name("table")) | table_name("table")) +
                    Optional(Optional(AS) + table_alias("table_alias")) +
                    Optional(INDEXED + BY + index_name("name") | NOT + INDEXED)("index") |
                  (LPAR + select_stmt + RPAR + Optional(Optional(AS) + table_alias)) |
                  (LPAR + join_source + RPAR) )

join_source << single_source + ZeroOrMore(join_op + single_source + join_constraint)


result_column = "*" | table_name + "." + "*" | (expr + Optional(Optional(AS) + column_alias))
select_core = (SELECT + Optional(DISTINCT | ALL) + Group(delimitedList(result_column))("columns") +
                Optional(FROM + join_source) +
                Optional(WHERE + expr("where_expr")) +
                Optional(GROUP + BY + (Group(delimitedList(ordering_term))("group_terms")) +
                        Optional(HAVING + expr("having_expr"))))

select_stmt << (select_core + ZeroOrMore(compound_operator + select_core) +
                Optional(ORDER + BY + Group(delimitedList(ordering_term))("order_by_terms")) +
                Optional(LIMIT + (integer |  integer + OFFSET + integer | integer + COMMA + integer))("limit_terms"))


def testcases():
    tests = """\
        select * from xyzzy where z > 100
        select * from xyzzy where z > 100 order by zz
        select * from xyzzy
        Select A from Sys where a=1
        select a from b where c=1 and b=2
        select a from b where c in (1, 2, 3) and d in ('a', 'b')
        select a,d from b where c=1 group by d
        select a,d from b where c=1 group by d limit 30
        """.splitlines()
    for t in tests:
        print t
        try:
            print select_stmt.parseString(t).dump()
        except ParseException, pe:
            print pe.msg
        print

def parse(query):
    try:
        result = select_stmt.parseString(query)
        print result.dump()
    except ParseException, pe:
        print pe.msg
    return result


if __name__ == '__main__':
    #testcases()
    for line in sys.stdin:
        try:
            print select_stmt.parseString(line).dump()
        except ParseException, pe:
            print pe.msg
        print

