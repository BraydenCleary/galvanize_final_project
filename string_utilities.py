import re

def lowercase(string):
  return str(string).lower()

def snakecase(string):
  string = re.sub(r"[\-\.\s]", '_', str(string))
  if not string:
      return string
  return lowercase(string[0]) + re.sub(r"[A-Z]", lambda matched: '_' + lowercase(matched.group(0)), string[1:])