package util.testkit

object EngineTestData {

  lazy val phrases = Stream(
    "if you can dream and not make dreams your master",
    "if you can think and not make thoughts your aim",
    "if you can meet with Triumph and Disaster",
    "and treat those two impostors just the same",
    "if you can bear to hear the truth you’ve spoken",
    "twisted by knaves to make a trap for fools",
    "or watch the things you gave your life to broken",
    "and stoop and build ’em up with worn-out tools"
  )


  lazy val mapperResult: Seq[(Any, Any)] = Seq(
    ("if",1), ("you",1), ("can",1), ("dream",1), ("and",1), ("not",1), ("make",1),
    ("dreams",1), ("your",1), ("master",1), ("if",1), ("you",1), ("can",1), ("think",1),
    ("and",1), ("not",1), ("make",1), ("thoughts",1), ("your",1), ("aim",1), ("if",1),
    ("you",1), ("can",1), ("meet",1), ("with",1), ("Triumph",1), ("and",1), ("Disaster",1),
    ("and",1), ("treat",1), ("those",1), ("two",1), ("impostors",1), ("just",1), ("the",1), ("same",1),
    ("if",1), ("you",1), ("can",1), ("bear",1), ("to",1), ("hear",1), ("the",1), ("truth",1), ("you’ve",1),
    ("spoken",1), ("twisted",1), ("by",1), ("knaves",1), ("to",1), ("make",1), ("a",1), ("trap",1), ("for",1),
    ("fools",1), ("or",1), ("watch",1), ("the",1), ("things",1), ("you",1), ("gave",1), ("your",1), ("life",1),
    ("to",1), ("broken",1), ("and",1), ("stoop",1), ("and",1), ("build",1), ("’em",1), ("up",1),
    ("with",1), ("worn-out",1), ("tools",1)
  )


  lazy val reducedWordCountOutput = Map(
    "you" -> 5, "and" -> 6, "can" -> 4, "if" -> 4, "make" -> 3, "the" -> 3, "to" -> 3, "your" -> 3,
    "not" -> 2, "with" -> 2, "Disaster" -> 1, "Triumph" -> 1, "a" -> 1, "aim" -> 1, "bear" -> 1, "broken" -> 1,
    "build" -> 1, "by" -> 1, "dream" -> 1, "dreams" -> 1, "fools" -> 1, "for" -> 1, "gave" -> 1, "hear" -> 1,
    "impostors" -> 1, "just" -> 1, "knaves" -> 1, "life" -> 1, "master" -> 1, "meet" -> 1, "or" -> 1, "same" -> 1,
    "spoken" -> 1, "stoop" -> 1, "things" -> 1, "think" -> 1, "those" -> 1, "thoughts" -> 1, "tools" -> 1, "trap" -> 1,
    "treat" -> 1, "truth" -> 1, "twisted" -> 1, "two" -> 1, "up" -> 1, "watch" -> 1, "worn-out" -> 1, "you’ve" -> 1, "’em" -> 1
  )
}
