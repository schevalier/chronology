import sys


class Parser(object):
  START_CODE = 1
  MAKE_CODE = 2
  END_CODE = 3

  def __init__(self, start_text, end_text, markdown_fmt):
    self.md_text = '#'
    self.start_text = start_text
    self.end_text = end_text
    self.start_fmt = '```%s' % markdown_fmt
    self.end_fmt = '```'

PYTHON = "python"
GO_LANG = "golang"
JAVASCRIPT = "javascript"

PARSERS = {
  PYTHON: Parser('"""', '"""', 'python'),
  GO_LANG: Parser('/*', '*/', 'golang'),
  JAVASCRIPT: Parser('/*', '*/', 'javascript')
}


def main(parser):
  state = parser.START_CODE
  output = []
  for line in sys.stdin:
    if state == parser.START_CODE and parser.start_text in line:
      state = parser.MAKE_CODE  # Ignore the first """.
    elif state == parser.MAKE_CODE and parser.end_text in line:
      output.append(parser.start_fmt)
      state = parser.END_CODE
    elif state == parser.END_CODE and parser.md_text in line:
      line = line.replace('\t', '')
      output.append(parser.end_fmt)
      output.append(line.rstrip())
      state = parser.START_CODE
    elif state == parser.END_CODE and parser.start_text in line:
      output.append(parser.end_fmt)
      state = parser.MAKE_CODE
    else:
      if state == parser.MAKE_CODE:
        line = line.replace('\t', '')
      output.append(line.rstrip())
  output.append(parser.end_fmt)

  remove_next = False
  # Cleanup pass
  for first, second in zip(output, output[1:] + ['']):
    if remove_next:
      remove_next = False
      continue
    elif first.strip() == '' and second == parser.end_fmt:
      continue
    elif first == parser.start_fmt and second == parser.end_fmt:
      remove_next = True
      continue
    print first

if __name__ == '__main__':
  parser_type = None
  if len(sys.argv) == 2:
    parser_type = sys.argv[1]
  parser = PARSERS.get(parser_type, PARSERS[PYTHON])
  main(parser)
