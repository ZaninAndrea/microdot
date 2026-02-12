#let report(
  title: none,
  date: datetime.today(),
  longForm: false,
  doc,
) = {
  let displayDate = date.display("[day] [month repr:long] [year]")

  // Document styling
  set document(title: title, date: date)
  let header = [#displayDate #h(1fr) #title]
  set page(paper: "us-letter", numbering: "1", number-align: center, header: header)
  set heading(numbering: "1.1")

  // Text styling
  let fontSize = 11pt
  set text(font: "Poppins", size: fontSize)
  show raw: set text(ligatures: true, font: "Cascadia Code", size: fontSize)
  set par(justify: true)

  // Heading styling
  show heading.where(level:1): it => {
    if longForm [
      #set align(center)
      #set text(size: 24pt)
      #pagebreak()
      #pad(top: 20pt, bottom: 30pt, [#counter(heading).display(it.numbering). #it.body])
    ] else [
      #counter(heading).display(it.numbering).
      #it.body
    ]
  }
  show heading.where(level:2): set text(size: 18pt) if longForm
  show heading.where(level:3): set text(size: 16pt) if longForm
  show heading.where(level:4): set text(size: 14pt) if longForm
  
  // Table of content styling
  show outline.entry.where(
    level: 1
  ): it => {
    v(12pt, weak: true)
    strong(it)
  }


  // Title page
  page(header: none, numbering:none)[
    #align(center, text(size:32pt, title))
    #align(center, text(size:20pt, displayDate))

    #pad(
      left:40pt,
      right:40pt,
      top: 120pt,
      outline(title:none, indent: 20pt)
    )
  ]

  // Code blocks
  show raw.where(block: true): set block(fill: luma(230), inset: 8pt, radius: 4pt, width: 100%)


  // Reset page counter to get accurate counts
  counter(page).update(1)
  doc
}

