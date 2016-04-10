:Class vecdb
⍝ Dyalog APL vector database - see https://github.com/Dyalog/vecdb

    (⎕IO ⎕ML)←1 1

    :Section Constants
    :Field Public Shared Version←'0.2.5' ⍝ Added ability to use Calculated columns
    :Field Public Shared TypeNames←,¨'I1' 'I2' 'I4' 'F' 'B' 'C'
    :Field Public Shared TypeNums←83 163 323 645 11 163
    :Field Public Shared SummaryFns←'sum' 'max' 'min' 'count'
    :Field Public Shared CalcFns←,⊂'map'
    :Field Public Shared SummaryAPLFns←'+/' '⌈/' '⌊/' '≢'
    :EndSection ⍝ Constants

    :Section Instance Fields            ⍝ The fact that these are public does not mean it is safe to change them
    :Field Public Name←''
    :Field Public Folder←''             ⍝ Where is it
    :Field Public BlockSize←100000      ⍝ Small while we test (must be multiple of 8)
    :Field Public NumBlocks←1           ⍝ We start with one block
    :Field Public noFiles←0             ⍝ in-memory database (not supported)
    :Field Public isOpen←0              ⍝ Not yet open
    :Field Public ShardFolders←⍬        ⍝ List of Shard Folders
    :Field Public ShardFn←⍬             ⍝ Shard Calculation Function
    :Field Public ShardCols←⍬           ⍝ ShardFn input column indices
    :Field Public ShardSelected←⍬       ⍝ Shards selected
    :Field Private AllShards←0          ⍝ Are all Shards in use?

    :Field _Columns←⍬
    :Field _Types←⍬
    :Field _Count←⍬

    :EndSection ⍝ Instance Fields

    fileprops←'Name' 'BlockSize' ⍝ To go in comp 4 of meta.vecdb
    eis←{(≡⍵)∊0 1:⊂,⍵ ⋄ ⍵}       ⍝ enclose if simple

    :Section Properties
    :Property Columns
    :Access Public
        ∇ r←get
          r←_Columns,_CalcCols
        ∇
    :EndProperty

    :Property Types
    :Access public
        ∇ r←get
          r←_Types,_CalcTypes
        ∇
    :EndProperty

    :Property Count
    :Access public
        ∇ r←get
          r←⊃+/_Counts.counter
        ∇
    :EndProperty
    :EndSection ⍝ Properties

    ∇ Open(folder)
      :Implements constructor
      :Access Public
     
      OpenFull(folder ⍬) ⍝ Open all shards
    ∇

    ∇ InitCalcs tn;i;calc;spec;space;inv
    ⍝ Extract calculation data from meta file
     
      :If 8=2⊃⎕FSIZE tn ⍝ If File format pre-dates calculated columns
          'unused'⎕FAPPEND tn ⍝ 8
          'unused'⎕FAPPEND tn ⍝ 9
          (⍬ ⍬ ⍬)⎕FAPPEND tn  ⍝ 10 Calc Col Names, Source Columns, Data Type
      :EndIf
     
      (_CalcCols _CalcSources _CalcTypes)←⎕FREAD tn,10 ⍝ Calculated column definitions
      mappings,←⎕NS¨(≢_CalcCols)⍴⊂'' ⍝ Add mappings
     
      :For i :In ⍳≢_CalcCols  ⍝ Run setup for each calculated coumn
          space←(i+≢_Columns)⊃mappings
          (calc spec inv)←⎕FREAD tn,10+i
          :If '{'=⊃calc             ⍝ User-defined
              space.Type←2          ⍝ Calculation
              space.Spec←spec       ⍝ Store data
              space⍎'Calc←',calc    ⍝ Define function
              :If 0≠⍴inv ⋄ space⍎'CalcInv←',inv ⋄ :EndIf ⍝ Define inverse
          :Else
              ⍎'(i⊃_CalcCols) ',calc,'_Setup spec'
          :EndIf
      :EndFor
    ∇

    ∇ name map_Setup(from to);cix;six;i;n;src;m;col;symbol;char;calcix
    ⍝ Setup for a "mapped" column
     
      (cix six)←Columns⍳(⊂name),_CalcSources[calcix←_CalcCols⍳⊂name]
      (col src)←mappings[cix six]
     
      :If (,'C')≡cix⊃Types                     ⍝ Special case char
      :AndIf (from≡⍳≢from)∨char←(,'C')≡six⊃Types   ⍝ char-char or "direct" map
     
          :If char
              symbol←src.symbol           ⍝ source symbols
              m←(≢from)≥i←from⍳symbol     ⍝ mappable symbols
              (m/symbol)←to[m/i]          ⍝ remap
          :Else
              symbol←to
          :EndIf
     
          col.file←0                      ⍝ there is no symbol file
          col.Type←1                      ⍝ Symbol
          col.symbol←symbol               ⍝ Store symbol list
          col.(SymbolIndex←symbol∘⍳)      ⍝ Create lookup function
          col.Source←six                  ⍝ Store the source column
     
      :Else
          col.Type←2                      ⍝ Calc/CalcInv
          col.(to from)←to from
          col.(SymbolIndex←from∘⍳)
          col.(TargetIndex←to∘⍳)
          col.(Calc←to∘{⍺⌷⍨⊂SymbolIndex ⍵})
          col.(CalcInv←from∘{⍺⌷⍨⊂TargetIndex ⍵})
      :EndIf
    ∇

    ∇ r←AddCalc spec;name;source;type;calc;file;tn;i;inv
      :Access Public
     
      'not allowed unless all shards are open'⎕SIGNAL AllShards↓11
      (name source type calc spec inv)←6↑,¨spec,⍬ ⍬ ⍬
     
      'unknown source column'⎕SIGNAL((⊂source)∊_Columns)↓11
      'unknown data type'⎕SIGNAL((⊂type)∊TypeNames)↓11
     
      :If '{'≠1⊃calc
          :Select calc
          :Case 'map'
              :If 2≠⍴spec
              :OrIf ≢/≢¨spec
                  'map source and target must have the same length'⎕SIGNAL 11
              :EndIf
          :Else
              ('Unknown standard calculation: ',calc)⎕SIGNAL 11
          :EndSelect
      :EndIf
     
      file←Folder,'meta.vecdb'
      ⎕FHOLD tn←file ⎕FSTIE 0
      (_CalcCols _CalcSources _CalcTypes)←⎕FREAD tn,10 ⍝ Calculated column definitions
     
      :If (≢_CalcCols)≥i←_CalcCols⍳⊂name ⍝ Existing source?
          (i⊃_CalcSources)←source
          (i⊃_CalcTypes)←type
          (_CalcCols _CalcSources _CalcTypes)⎕FREPLACE tn,10
          (calc spec inv)⎕FREPLACE tn,10+i
     
      :Else                              ⍝ New source
          ((_CalcCols _CalcSources _CalcTypes),∘⊂¨name source type)⎕FREPLACE tn,10
          :If (10+i)=2⊃⎕FSIZE tn         ⍝ Append or replace?
              (calc spec inv)⎕FAPPEND tn
          :Else ⋄ (calc spec inv)⎕FREPLACE tn,10+i
          :EndIf
      :EndIf
     
      InitCalcs tn ⍝ re-read all from file (optimise later if necessary)
      ⎕FUNTIE tn ⍝ Also unholds it
    ∇

    ∇ r←RemoveCalc name;file;tn;m;i;cn
      :Access Public
     
      'not allowed unless all shards are open'⎕SIGNAL AllShards↓11
      'calc not found'⎕SIGNAL((⊂name)∊_CalcCols)↓11
     
      file←Folder,'meta.vecdb'
      ⎕FHOLD tn←file ⎕FSTIE 0
      (_CalcCols _CalcSources _CalcTypes)←⎕FREAD tn,10 ⍝ Calculated column definitions
      i←(m←~_CalcCols∊⊂name)⍳0
      (m∘/¨_CalcCols _CalcSources _CalcTypes)⎕FREPLACE tn,10
     
      :For cn :In ⌽i↓10+⍳≢_CalcCols
          (⎕FREAD tn,cn)⎕FREPLACE tn,cn-1 ⍝ Copy following specs down
      :EndFor
     
      :If (11+≢_CalcCols)=2⊃⎕FSIZE tn      ⍝ Did we stop using the last component?
          ⎕FDROP tn,¯1
      :EndIf
     
      InitCalcs tn ⍝ re-read all from file (optimise later if necessary)
      ⎕FUNTIE tn ⍝ Also unholds it
    ∇

    ∇ r←Calc(name data);i;ns;cix;src;col
      :Access Public
     
      'calculation not found'⎕SIGNAL((≢_CalcCols)<i←_CalcCols⍳⊂name)⍴11
     
      col←(cix←i+≢_Columns)⊃mappings
      :Select col.Type
      :Case 1 ⍝ Symbol
          src←mappings[col.Source]
          r←col.symbol[src.SymbolIndex data]
      :Case 2 ⍝ Calc
          :Trap 999
              r←col.Calc data
          :Else
              (⊃⎕DMX.DM)⎕SIGNAL ⎕DMX.EN
          :EndTrap
      :Else
          ∘∘∘ ⍝ Internal error - unknown mapping type
      :EndSelect
    ∇

    ∇ OpenFull(folder shards);tn;file;props;shards;n;s;i
    ⍝ Open an existing database
     
      :Implements Constructor
      :Access Public
     
      folder←AddSlash folder
      shards←,shards
     
      :Trap 0 ⋄ tn←(file←folder,'meta.vecdb')⎕FSTIE 0
      :Else ⋄ ('Unable to open ',file)⎕SIGNAL 11
      :EndTrap
      (props(_Columns _Types)ShardFolders(ShardFn ShardCols))←⎕FREAD tn(4 5 6 7)
      n←≢_Columns
      mappings←⎕NS¨n⍴⊂''
      mappings.Type←0 ⍝ Not cal'd or mapped (yet)
      InitCalcs tn
      ⎕FUNTIE tn
     
      ⍎'(',(⍕1⊃props),')←2⊃props'
      s←≢ShardFolders
     
      :If 0=⍴shards     ⍝ No explicit selection of shards
          ShardSelected←⍳s
      :Else
          'Invalid Shard Selection'⎕SIGNAL(∧/shards∊⍳s)↓11
          ShardSelected←shards
      :EndIf
      AllShards←s=≢ShardSelected
     
      Shards←⎕NS¨¨s⍴⊂n⍴⊂''
      Shards.name←s⍴⊂_Columns                ⍝ Column Names
      Shards.type←s⍴⊂_Types                  ⍝ Types
      Shards.file←(n/¨⊂¨ShardFolders),¨¨(,∘'.vector')¨¨s⍴⊂(⍕¨⍳n) ⍝ Vector file names
     
      :If 0≠⍴ShardFn ⋄ findshard←⍎ShardFn ⋄ :EndIf ⍝ Define shard calculation function
     
      :For i :In {⍵/⍳⍴⍵}'C'=⊃¨_Types         ⍝ Read symbol files for CHAR fields
          col←i⊃mappings
          col.Type←1                         ⍝ symbol map
          col.file←folder,(⍕i),'.symbol'     ⍝ symbol file name in main folder
          col.symbol←GetSymbols col.file     ⍝ Read symbols
          col.(SymbolIndex←symbol∘⍳)         ⍝ Create lookup function
      :EndFor
     
      (isOpen Folder)←1 folder
      MakeMaps
    ∇

    ∇ MakeMaps;s;i;types;T;ns;dr;col;sizes
    ⍝ [Re]make all maps
      types←TypeNums[TypeNames⍳Types]
      _Counts←⎕NS¨(≢Shards)⍴⊂⍬
     
      :For i :In ShardSelected
          s←i⊃Shards
          _Counts[i].counter←645 1 ⎕MAP((i⊃ShardFolders),'counters.vecdb')'W' ⍝ Map record counter
          :For col :In ⍳≢s
              (col⊃s).vector←(types[col],¯1)⎕MAP(col⊃s).file'W'
          :EndFor
     
          :If 1≠⍴sizes←∪s.(≢vector) ⍝ mapped vectors have different lengths
          :OrIf sizes∨.<⊃(i⊃_Counts).counter ⍝ or shorter than record count
              ∘ ⍝ File damaged
          :EndIf
      :EndFor
    ∇

    ∇ make4(name folder columns types)
      :Implements constructor
      :Access Public
      0 CreateOrExtend name folder columns types'' '' ⍝ No data or option
      Open,⊂folder      ⍝ now open it properly
    ∇

    ∇ make5(name folder columns types options)
      :Implements constructor
      :Access Public
      0 CreateOrExtend name folder columns types options'' ⍝ No data or option
      Open,⊂folder      ⍝ now open it properly
    ∇

    ∇ make6(name folder columns types options data)
      :Implements constructor
      :Access Public
      0 CreateOrExtend name folder columns types options data
      Open,⊂folder      ⍝ now open it properly
    ∇

    ∇ extend CreateOrExtend(name folder columns types options data);i;s;offset;tn;type;length;col;size;n;dr;f;shards;sf;create;newcols;metafile;dix;d;newchars
    ⍝ Create (extend=0) a new database or extend an existing one
    ⍝ Called from constructors and public method AddColumns
      create←extend=0   ⍝ for readability
     
    ⍝ Validate column parameters
      types←,¨types
      'At least one column must be added'⎕SIGNAL(1>≢columns)⍴11
      'Column types and names do not have same length'⎕SIGNAL((≢columns)≠≢types)⍴11
      'Invalid column types - see vecdb.TypeNames'⎕SIGNAL(∧/types∊TypeNames)↓11
      'Column(s) already exist'⎕SIGNAL(∨/columns∊_Columns)⍴11
      :If 0=≢data ⋄ data←(≢columns)⍴⊂⍬ ⋄ :EndIf ⍝ Default data is all zeros
      'Data lengths not all the same'⎕SIGNAL(1≠≢length←∪≢¨data)/11
     
      folder,←((¯1↑folder)∊'/\')↓'/' ⍝ make sure we have trailing separator
      metafile←folder,'meta.vecdb'
     
      :If create ⍝ We are CREATEing a database
          :If Exists ¯1↓folder ⍝ Folder already exists
              ('"',metafile,'" already exists')⎕SIGNAL(Exists metafile)/11
          :Else ⍝ Folder does not exist
              :Trap 0 ⋄ MkDir ¯1↓folder
              :Else ⋄ ⎕DMX.Message ⎕SIGNAL ⎕DMX.EN
              :EndTrap
          :EndIf
          ProcessOptions options ⍝ Sets global fields
          'Block size must be a multiple of 8'⎕SIGNAL(0≠8|BlockSize)/11
     
      ⍝ Set defaults for sharding (1 shard)
          ShardFolders,←(0=⍴ShardFolders)/⊂folder
          ShardFolders←AddSlash¨ShardFolders
          ShardCols←,ShardCols
          :If 0≠⍴ShardFn ⋄ findshard←⍎ShardFn ⋄ :EndIf  ⍝ Define shard calculation function
          (Name _Columns _Types)←name columns types     ⍝ Set Class fields
          mappings←⎕NS¨(≢_Columns)⍴⊂''
     
      :Else ⍝ We are adding columns to an open database
          (_Columns _Types)←(_Columns _Types),¨columns types ⍝ Extend Class fields
          mappings,←⎕NS¨(≢columns)⍴⊂''
     
      :EndIf
      newcols←(-≢columns)↑⍳≢_Columns         ⍝ Indices of new coulumns
      newchars←'C'=⊃¨_Types[newcols]         ⍝ /// Should really be driven off mappings.Type=1 in the future
     
      :For i :In newchars/newcols ⍝ Create symbol files for CHAR fields
          col←i⊃mappings
          dix←newcols⍳i                       ⍝ data index
          col.symbol←∪dix⊃data                ⍝ Unique symbols in input data
          col.file←folder,(⍕i),'.symbol'      ⍝ Symbol file name in main folder
          col.symbol PutSymbols col.file      ⍝ Read symbols
          col.(SymbolIndex←symbol∘⍳)          ⍝ Create lookup function
          (dix⊃data)←col.SymbolIndex dix⊃data ⍝ Convert indices
      :EndFor
     
      :If create
          (shards data)←newcols ShardData data ⍝ NB data has one COLUMN per shard
          data←data,⊂⍬
      :Else ⍝ adding columns
          shards←⍳≢Shards
          data←((≢newcols),≢shards)⍴⊂⍬ ⍝ No data provided when adding cols
      :EndIf
     
      :For f :In ⍳≢ShardFolders
          :If ~Exists sf←f⊃ShardFolders ⋄ MkDir sf ⋄ :EndIf
     
          d←data[;shards⍳f]             ⍝ extract records for one shard
          :If create
          n←≢⊃d
          :Else
              n←f⊃_Counts.counter
          :EndIf
          size←BlockSize×1⌈⌈n÷BlockSize ⍝ At least one block
     
          :If create                    ⍝ # of records in the shard
              tn←(sf,'counters.vecdb')⎕NCREATE 0
              n ⎕NAPPEND tn 645         ⍝ Record the number of records as a FLOAT
              ⎕NUNTIE tn
          :EndIf
     
          :For i :In newcols            ⍝ For each column being added
              dr←(TypeNames⍳_Types[i])⊃TypeNums
              tn←(sf,(⍕i),'.vector')⎕NCREATE 0
              (size↑(newcols⍳i)⊃d)⎕NAPPEND tn dr
              ⎕NUNTIE tn
          :EndFor
      :EndFor
     
      :If create
          tn←metafile ⎕FCREATE 0
          ('vecdb ',Version)⎕FAPPEND tn    ⍝ 1
          'See github.com/Dyalog/vecdb/doc/Implementation.md'⎕FAPPEND tn ⍝ 2
          'unused'⎕FAPPEND tn              ⍝ 3
          (fileprops(⍎¨fileprops))⎕FAPPEND tn ⍝ 4 (Name BlockSize)
          (_Columns _Types)⎕FAPPEND tn     ⍝ 5
          ShardFolders ⎕FAPPEND tn         ⍝ 6
          (ShardFn ShardCols)⎕FAPPEND tn   ⍝ 7
     
      :Else ⍝ Extending
          tn←metafile ⎕FTIE 0
          (_Columns _Types)⎕FREPLACE tn 5
      :EndIf
      ⎕FUNTIE tn
    ∇

    ∇ (shards data)←cix ShardData data;six;s;char;rawdata;sym;c;counts;m
     ⍝ Shards is a vector of shards to be updated
     ⍝ data has one column per shard, and one row per column
     
      rawdata←data
      :If 1=≢ShardFolders ⍝ Data will necessarily all be in the 1st shard then!
          shards←,1 ⋄ data←⍪data
     
      :Else ⍝ Database *is* sharded
          :If (≢cix)∨.<six←cix⍳ShardCols ⍝ Shard columns not present - we must be adding columns?
              ⍝ ∘∘∘ does not work ∘∘∘
              'Record count is incorrect'⎕SIGNAL((≢⊃data)≠+/counts←⊃¨_Counts.counter)⍴11
              shards←⍳⍴Shards
              m←(+/counts)⍴0 ⋄ m[+\1,¯1↓counts]←1
              data←↑m∘⊂¨data
     
          :Else
              char←{⍵/⍳⍵}'C'=⊃¨_Types[ShardCols] ⍝ Which of the sharding cols are of type char?
              ⍝ /// ↑ should be driven off 1=mapping.Type in the future
     
              :If (1=≢char)∧1=≢six ⍝ There is exactly one char shard column...
              ⍝ See whether it is worth running shard function on unique values rather than all data:
              :AndIf (≢⊃sym←mappings[ShardCols].symbol)<≢⊃data ⍝ ... and fewer unique symbols than records
                  s←{⍺ ⍵}⌸(findshard sym)[six⊃data]           ⍝ ... then compute shards on symbols
     
              :Else                ⍝ General case
     
                  :If 0≠≢char ⍝ Are *any* char (then we must turn indices into text)
                      c←six[char] ⍝ index of character shard cols in provided data
                      data[c]←mappings[ShardCols[char]].{symbol[⍵]}data[c]
                      ⍝ Is that faster than: data[c]←mappings[ShardCols[char]].symbol[data[c]]
                  :EndIf
     
                  s←{⍺ ⍵}⌸findshard data[six]
              :EndIf
     
              shards←s[;1]
              data←↑[0.5](⊂∘⊂¨s[;2])⌷¨¨⊂rawdata
          :EndIf
      :EndIf
    ∇

    ∇ ExtendShard(folder cols count data);i;file;tn;Type;char;tns;sym;m;ix;fp;dr;col
    ⍝ Extend a Shard by count items
     
      :For i :In ⍳≢cols ⍝ For each column
          col←i⊃cols
          dr←(TypeNames⍳⊂col.type)⊃TypeNums
          col.⎕EX'vector'                    ⍝ Remove memory map
          tn←col.file ⎕NTIE 0
          (count↑i⊃data)⎕NAPPEND tn,dr
          ⎕NUNTIE tn
          col.vector←(dr,¯1)⎕MAP col.file'W' ⍝ Re-establish map
      :EndFor
    ∇

    ∇ r←Close
      :Access Public
      ⎕EX'Shards' 'mappings' '_Counts'
      r←isOpen←0       ⍝ record the fact
    ∇

    ∇ unmake
      :Implements Destructor
      {}Close
    ∇

    ∇ ProcessOptions options;name
    ⍝ Extract optional fields from options
     
      :If 9=⎕NC'options'
          :For name :In options.⎕NL-2
              :If (⊂name)∊'BlockSize' 'InitBlocks' 'Folders' 'ShardCols' 'ShardFolders' 'ShardFn'
                  ⍎name,'←options.',name
              :Else
                  ('Invalid option name: ',name)⎕SIGNAL 11
              :EndIf
          :EndFor
      :EndIf
    ∇

    ∇ (summary colnames)←ParseSummary cols;p
    ⍝ Split column specifications into summaryfn colname
     
      :If 0=⍴cols ⋄ summary←colnames←⍬
      :Else
          :If 2>≡cols ⋄ cols←,⊂,cols ⋄ :EndIf ⍝ Enclose if simple
          p←p×(≢¨cols)≥p←cols⍳¨' ' ⍝ position of separator
          summary←(0⌈p-1)↑¨cols
          colnames←p↓¨cols
      :EndIf
    ∇

    ∇ {r}←AddColumns(columns types);z
      :Access Public
     
      'not allowed unless all shards are open'⎕SIGNAL AllShards↓11
      1 CreateOrExtend Name Folder columns types''⍬
      z←Close ⋄ Open,⊂Folder ⍝ Reopen - might want to optimise this later?
      r←⍬
    ∇

    ∇ {r}←RemoveColumns columns;tn;keep;metafile;f;c;colix;file;sf;m;sym
      :Access Public
     
      'not allowed unless all shards are open'⎕SIGNAL AllShards↓11
      :If ∨/m←~columns∊_Columns
          ('Columns not found:',⍕m/columns)⎕SIGNAL 11
      :EndIf
     
      'Cannot remove sharding columns'⎕SIGNAL(∨/columns∊_Columns[ShardCols])⍴11
      'Cannot remove all columns'⎕SIGNAL(∧/_Columns∊columns)⍴11
     
      keep←~_Columns∊columns
     
      ⎕EX'Shards' ⍝ We will reopen the file at the end, need to remove maps
     
      :For f :In ⍳≢ShardFolders
          colix←1
          sf←f⊃ShardFolders
          :For c :In ⍳≢_Columns
              tn←(file←sf,(⍕c),'.vector')⎕NTIE 0
              sym←{22::0 ⋄ (Folder,(⍕c),'.symbol')⎕NTIE ⍵}0
              :If c⊃keep ⍝ keeping this column
                  :If c≠colix ⍝ needs renaming
                      (sf,(⍕colix),'.vector')⎕NRENAME tn
                      :If (f=1)∧sym≠0
                          (Folder,(⍕colix),'.symbol')⎕NRENAME sym
                      :EndIf
                  :EndIf
                  colix+←1
              :Else      ⍝ erasing this column
                  file ⎕NERASE tn
                  :If (f=1)∧sym≠0
                      (Folder,(⍕c),'.symbol')⎕NERASE sym
                  :EndIf
              :EndIf
              ⎕NUNTIE ⎕NNUMS∩tn,sym
          :EndFor
      :EndFor
     
      (_Columns _Types)←keep∘/¨_Columns _Types
     
      metafile←Folder,'meta.vecdb'
      tn←metafile ⎕FTIE 0
      (_Columns _Types)⎕FREPLACE tn 5
      ⎕FUNTIE tn
      {}Close ⋄ Open,⊂Folder ⍝ Reopen
      r←⍬
    ∇

    ∇ r←Query args;where;cols;groupby;col;value;ix;j;s;count;Data;Cols;summary;m;i;f;cix;calc;mapped;c;columns;map
      :Access Public
     
      (where cols groupby)←3↑args,(≢args)↓⍬ ⍬ ⍬
      cols←(0≠≢cols)/,,¨eis cols
      columns←Columns
      :If 2=≢where ⋄ :AndIf where[1]∊columns ⍝ just a single constraint?
          where←,⊂where
      :EndIf
     
      (summary cols)←ParseSummary cols
      'UNKNOWN SUMMARY FUNCTION'⎕SIGNAL(∧/summary∊SummaryFns,⊂'')↓11
     
      :If 0≠≢groupby ⍝ We are grouping
          :If 1=≡groupby ⋄ groupby←,⊂groupby ⋄ :EndIf ⍝ Enclose if simple
          m←(0≠≢¨summary)∨cols∊groupby ⍝ summary or one of the grouping cols?
          'ONLY SUMMARIZED COLUMNS MAY BE SELECTED WHEN GROUPING'⎕SIGNAL(∧/m)↓11
      :EndIf
     
      r←0 2⍴0 ⍝ (shard indices)
     
      :For s :In ShardSelected
          Cols←s⊃Shards
          count←⊃(s⊃_Counts).counter
          ix←⎕NULL
     
          :For (col value) :In where ⍝ AND them all together
     
              :If (≢columns)<j←columns⍳⊂col
                  ('Invalid column name in where clause: ',⍕col)⎕SIGNAL 11
              :EndIf
     
              map←mappings[j]
              mapped←1=map.Type  ⍝ Mapped or Calculated
              f←⊢
     
              :If calc←(≢_CalcCols)≥cix←_CalcCols⍳⊂col
                  :If mapped
                      value←(map.symbol∊value)/mappings[_Columns⍳_CalcSources[cix]].symbol
                      calc←0 ⍝ Do not calculate, we did the map already
                  :ElseIf 3=⎕NC'map.CalcInv' ⍝ Are we able to calculate the inverse?
                      value←map.CalcInv value
                      calc←0 ⍝ Do not calculate: Just search for result of inverse
                  :Else ⍝ We will need to apply the function to data
                      f←map.Calc
                  :EndIf
                  j←_Columns⍳_CalcSources[cix]
              :EndIf
     
              :If mapped ⍝ Char field, or 'map' calculation
                  value←mappings[j].SymbolIndex value    ⍝ v15.0: (j⊃mappings)⍳value
              :EndIf
     
              :If calc ⍝ need to compare with f(x) rather than x
                  :If ⎕NULL≡ix ⍝ First time round the loop: Compare all values
                      ix←{⍵/⍳⍴⍵}(f count↑Cols[j].vector)∊value
                  :Else ⋄ ix/⍨←(f Cols[j].vector[ix])∊value
                  :EndIf
              :Else    ⍝ /// block repeated without f in case f←⊢ would materialise data in ws
                  :If ⎕NULL≡ix ⋄ ix←{⍵/⍳⍴⍵}(count↑Cols[j].vector)∊value
                  :Else ⋄ ix/⍨←(Cols[j].vector[ix])∊value
                  :EndIf
              :EndIf
     
              :If 0=⍴ix ⋄ :Leave ⋄ :EndIf
          :EndFor ⍝ Clause
     
          r⍪←s ix
      :EndFor ⍝ Shard
     
      :If 0=≢cols ⋄ :GoTo 0 ⍝ Not asked to return anything: Just return indices
      :ElseIf 0=≢groupby    ⍝ no group by statement
          r←Read r cols
          :For i :In (0≠≢¨summary)/⍳≢cols
              (i⊃r)←⍎((SummaryFns⍳summary[i])⊃SummaryAPLFns),'i⊃r'
          :EndFor
      :Else
          r←Summarize r summary cols groupby
      :EndIf
    ∇

    ∇ r←Summarize(ix summary cols groupby);char;m;num;s;indices;fns;cix;allix;allcols;numrecs;blksize;offset;groupfn;t;multi;split;data;recs;groupix;colix;z;sourceix;sourcecols;mapix;calccols;calcix;c
      ⍝ Read and Summarize specified indices of named columns
      ⍝ Very similar to Read, but not public - called by Query
     
      allix←_Columns⍳allcols←groupby∪cols
      :If 0≠⍴calccols←((≢_CalcCols)≥calcix←_CalcCols⍳allcols)/⍳⍴allix
          allix[calccols]←_Columns⍳_CalcSources[calcix[calccols]] ⍝ source columns for calculated cols
      :EndIf
     
      groupix←allcols⍳groupby
      colix←allcols⍳cols
     
      fns←(SummaryAPLFns,⊂'')[SummaryFns⍳summary]
     
      :If 1=≢cols ⍝ Only one summarized column
          groupfn←⍎'{(↑[0.5]⍺){⍺,',(1⊃fns),'⍵}⌸⊃⍵}'
      :Else       ⍝ More than one summarized column
          z←⊂'r←keys groupfn data'
          :If 1=≢groupix ⋄ z,←⊂'keys←⊃keys' ⋄ :Else ⋄ z,←⊂'keys←↑[0.5]keys' ⋄ :EndIf
          z,←⊂'r←',(⍕≢groupix,colix),'↑⍤1⊢keys{⍺,',(1⊃fns),'⍵}⌸⊃data'
          z,←(1↓⍳≢colix){'r[;',(⍕⍺+≢groupix),']←keys{',⍵,'⍵}⌸',(⍕⍺),'⊃data'}¨1↓fns
          :If 'groupfn'≢⎕FX z ⋄ ∘∘∘ ⋄ :EndIf
      :EndIf
     
      r←(0,≢allix)⍴0
     
      :For (s indices) :In ↓ix
          offset←0
          :If indices≡⎕NULL ⍝ All records selected
              blksize←numrecs←⊃(s⊃_Counts).counter
          :Else ⍝ <indices> records selected
              blksize←numrecs←≢indices
          :EndIf
     
          split←0     ⍝ We did it all at once
          :Repeat
              :Trap 1 ⍝ WS FULL
                  recs←blksize⌊numrecs-offset
                  :If indices≡⎕NULL ⍝ All records still selected
                      data←offset((s⊃Shards)[allix].{⍵↑⍺↓vector})recs
                  :Else
                      data←(s⊃Shards)[allix].{vector[⍵]}⊂recs↑offset↓indices
                  :EndIf
     
                  :For c :In calccols ⍝ /// equivalent code exists in Read: refactor someday?
                      (c⊃data)←mappings[(≢_Columns)+calcix[c]].Calc c⊃data
                  :EndFor
     
                  r⍪←data[groupix]groupfn data[colix]
                  offset+←blksize
                  ⎕EX'data'
              :Else ⍝ Got a WS FULL
                  split←1 ⍝ We had to go around again
                  blksize←blksize(⌈÷)2
                  ⎕←(⍕⎕AI[3]),': block size reduced: ',⍕blksize
                  :If blksize<100000
                      ∘∘∘
                  :EndIf
              :EndTrap
          :Until offset≥numrecs
     
          :If split ⍝ re-summarize partial results
              r←r[;groupix]groupfn r[;colix]
          :EndIf
     
          :For char :In {⍵/⍳⍴⍵}'C'=⊃¨Types[(≢groupby)↑allix] ⍝ Symbol Group By cols
              r[;char]←mappings[allix[char]].{symbol[⍵]}r[;char]
          :EndFor
      :EndFor
    ∇

    ∇ r←Read(ix cols);char;m;num;cix;s;indices;t;calcix;calccols;c;nss;six;tix
      ⍝ Read specified indices of named columns
      :Access Public
     
      :If 1=⍴⍴ix ⋄ ix←1,⍪⊂ix ⋄ :EndIf    ⍝ Single Shard?
      :If 1=≡cols ⋄ cols←,⊂cols ⋄ :EndIf ⍝ Single simple column name
      ⎕SIGNAL/ValidateColumns cols
     
      tix←six←cix←Columns⍳cols
      :If 0≠⍴calccols←((≢_CalcCols)≥calcix←_CalcCols⍳cols)/⍳⍴cols
          six[calccols]←_Columns⍳_CalcSources[calcix[calccols]] ⍝ source columns for calculated cols
      :EndIf
      r←(⍴cix)⍴⊂⍬
     
      'Data found in unopened shard!'⎕SIGNAL(∧/ix[;1]∊ShardSelected)↓11
      :For (s indices) :In ↓ix
          :If indices≡⎕NULL ⋄ r←r,¨(s⊃_Counts).counter↑¨(s⊃Shards)[six].vector
          :Else ⋄ r←r,¨(s⊃Shards)[six].{vector[⍵]}⊂indices ⋄ :EndIf
      :EndFor
     
      :If 0≠⍴char←{⍵/⍳≢⍵}'C'=⊃¨Types[cix] ⍝ Symbol transation
      :AndIf 0≠⍴char←(m←2=⊃¨(nss←mappings[cix[char]]).⎕NC⊂'symbol')/char
          r[char]←(m/nss).{symbol[⍵]}r[char]
      :EndIf
     
      :For c :In calccols~char            ⍝ Exclude char-char maps handled above
          (c⊃r)←mappings[cix[c]].Calc c⊃r
      :EndFor
    ∇

    ∇ r←ValidateColumns cols;bad
     ⍝ Return result suitable for ⎕SIGNAL/
     
      r←''⍬
      :If ~0∊⍴bad←cols~Columns
          r←('Unknown Column Names:',,⍕bad)11
      :EndIf
    ∇

    ∇ r←Append(cols data);length;canupdate;shards;s;growth;tn;cix;count;i;append;Cols;size;d;n
      :Access Public
     
      'Data lengths not all the same'⎕SIGNAL(1≠≢length←∪≢¨data)/11
      'Col and Data counts not the same'⎕SIGNAL((≢cols)≠≢data)/11
      ⎕SIGNAL/ValidateColumns cols
     
      cix←_Columns⍳cols
      data←cix IndexSymbols data ⍝ Char to Symbol indices
     
      (shards data)←(⍳≢_Columns)ShardData data
      'data may only be appended to opened shards'⎕SIGNAL(∧/shards∊ShardSelected)↓11
     
      :For s :In shards
          d←data[;shards⍳s]
          length←≢⊃d              ⍝ # records to be written to *this* Shard
          Cols←s⊃Shards           ⍝ Mapped columns in this Shard
          count←⊃(s⊃_Counts).counter ⍝ Active records in this Shard
          size←≢Cols[⊃cix].vector ⍝ Current Shard allocation
     
          :If 0≠canupdate←length⌊size-count  ⍝ Updates to existing maps
              i←⊂count+⍳canupdate
              i(Cols[cix]).{vector[⍺]←⍵}canupdate↑¨d
          :EndIf
     
          :If length>canupdate               ⍝ We need to extend the file
              append←(≢_Columns)⍴⊂⍬
              append[cix]←canupdate↓¨d       ⍝ Data which was not updated
              growth←BlockSize×(length-canupdate)(⌈÷)BlockSize ⍝ How many records to add to the Shard
              ExtendShard(s⊃ShardFolders)Cols growth append
          :EndIf
     
          _Counts[s].counter[1]←count+length ⍝ Update (mapped) counter
      :EndFor
     
      r←0
    ∇

    ∇ {r}←Update(ix cols data);cix;indices;s;p;i
      :Access Public
     
      :If 1=≡cols ⋄ (cols data)←,∘⊂¨cols data ⋄ :EndIf ⍝ Simple col name
      ⎕SIGNAL/ValidateColumns cols
      cix←Columns⍳cols
      'Cannot update Sharding Cols'⎕SIGNAL(cix∊ShardCols)/11
     
      data←cix IndexSymbols data
     
      :If 1=≢ix ⋄ data←⍪data ⍝ One shard
      :Else                  ⍝ Partition data by Shard
          p←(≢⊃data)⍴0 ⋄ p[+\1,≢¨¯1↓ix[;2]]←1
          data←↑p∘⊂¨data
      :EndIf
     
      'data must be in opened shards!'⎕SIGNAL(∧/ix[;1]∊ShardSelected)↓11
      :For i :In ⍳≢ix        ⍝ Each shard
          (s indices)←ix[i;]
          (⊂indices)((s⊃Shards)[cix]).{vector[⍺]←⍵}data[;i]
      :EndFor
      r←0
    ∇

    ∇ r←Delete folder;file;tn;folders;files;f
      :Access Public Shared
      ⍝ Erase a vecdb file without opening it first (it might be too damaged to open)
      ⍝   Does check whether there is a meta file in the folder
      ⍝   Also deletes
     
      folder←AddSlash folder
      'Folder not found'⎕SIGNAL(DirExists folder)↓22           ⍝ Not there
      'Not a vecdb'⎕SIGNAL(Exists file←folder,'meta.vecdb')↓22 ⍝ Paranoia
     
      :If isWindows
          ⎕CMD'rmdir "',folder,'" /s /q'
      :Else
          1 _SH'rm -r ',folder
      :EndIf
     
      r←~DirExists folder
    ∇

    ∇ r←Erase
      :Access Public
      ⍝ /// needs error trapping
     
      'all shards must be open'⎕SIGNAL AllShards↓11
      'vecdb is not open'⎕SIGNAL isOpen↓11
     
      {}Close
      {}Delete Folder
      r←0
    ∇

    ∇ ix←ns SymbolUpdate values;m
      ⍝ Convert values to symbol indices, and update the file if necessary
     
      :If ∨/m←(≢ns.symbol)<ix←ns.SymbolIndex values   ⍝ new strings found
          'new strings not allowed unless all shards are open'⎕SIGNAL AllShards↓11
          ns.symbol,←∪m/values             ⍝ Update in-memory symbol table
          ns.symbol PutSymbols ns.file     ⍝ ... update the symbol file
          ns.(SymbolIndex←symbol∘⍳)        ⍝ ... define new hashed lookup function
          ix←ns.SymbolIndex values         ⍝ ... and use it
      :EndIf
    ∇

    ∇ data←cix IndexSymbols data;char
    ⍝ Convert all char columns to indices
     
      :If 0≠⍴char←{⍵/⍳⍴⍵}'C'=⊃¨_Types[cix]
          data[char]←mappings[cix[char]]SymbolUpdate¨data[char]
      :EndIf
     
    ∇

    ∇ r←GetSymbols file;tn;s
    ⍝ Read and deserialise symbol table from native file
     
      tn←file ⎕NTIE 0 ⋄ s←⎕NREAD tn 83,⎕NSIZE tn ⋄ ⎕NUNTIE tn
      :Trap 0 ⋄ r←0(220⌶)s ⍝ Deserialise
      :Else ⋄ ∘ ⋄ :EndTrap ⍝ Symbol table damaged :-(
    ∇

    ∇ r←symbols PutSymbols file;tn
    ⍝ Serialise and write symbol table to native file
     
      'SYMBOL TABLE FULL'⎕SIGNAL(32767<≢symbols)/11
     
      :Trap 22
          tn←file ⎕NTIE 0 ⋄ 0 ⎕NRESIZE tn
      :Else ⋄ tn←file ⎕NCREATE 0 ⋄ :EndTrap
      (1(220⌶)symbols)⎕NAPPEND tn 83 ⍝ Serialise and append
      ⎕NUNTIE tn
    ∇

    :Section Files
    ⍝ Much of this can be lost in Dyalog 15.0 when new Cross-platform File System Functions Arrive :-)

    ∇ r←isWindows
      r←'W'=3 1⊃'.'⎕WG'APLVersion'
    ∇

    ∇ f←unixfix f
    ⍝ replaces Windows file separator \ with Unix file separator /
    ⍝ this approach is mindnumbingly simple and probably dangerous
    ⍝ which is why we call unixfix very cautiously
      :If (⊂APLVersion)∊'*nix' 'Mac' ⋄ ((f='\')/f)←'/' ⋄ :EndIf
    ∇

    ∇ r←AddSlash path
    ⍝ Ensure folder name has trailing slash
      r←path,((¯1↑path)∊'/\')↓⊃isWindows⌽'/\'
    ∇

    ∇ r←Exists path;GFA
    ⍝ Is the argument the name of an existing file or folder?
      :Select APLVersion
      :Case 'Win'
          'GFA'⎕NA'U4 kernel32.C32|GetFileAttributes* <0T '
          r←(¯1+2*32)≢GFA⊂path
      :Else
          r←1
          :Trap 22
              :Trap 19 ⍝ file access error means file exists
                  ⎕NUNTIE(unixfix path)⎕NTIE 0
              :EndTrap
          :Else
              r←0
          :EndTrap
      :EndSelect
    ∇

    ∇ r←DirExists path;GFA
      r←0
      :Select APLVersion
      :CaseList '*nix' 'Mac'
          :Trap 11
              r←0<⍴_SH'ls -adl ',unixfix path
          :EndTrap
      :Case 'Win'
          'GFA'⎕NA'U4 kernel32.C32|GetFileAttributes* <0T '
          r←⊃2 16⊤GFA⊂path
      :EndSelect
    ∇

    ∇ MkDir path;CreateDirectory;GetLastError;err
      ⍝ Create a folder
      :Select APLVersion
      :CaseList '*nix' 'Mac'
          :If ~DirExists path
              1 _SH'mkdir ',unixfix path
              ('mkdir error on ',path)⎕SIGNAL 11/⍨~DirExists path
          :EndIf
      :Case 'Win'
          ⎕NA'I kernel32.C32∣CreateDirectory* <0T I4' ⍝ Try for best function
          →(0≠CreateDirectory path 0)⍴0 ⍝ 0 means "default security attributes"
          ⎕NA'I4 kernel32.C32|GetLastError'
          err ⎕SIGNAL⍨'CreateDirectory error:',⍕err←GetLastError
      :EndSelect
    ∇

    ∇ {r}←{suppress}_SH cmd
    ⍝ SH cover to suppress any error messages
    ⍝ suppress will suppress error from being signaled
      :If 0=⎕NC'suppress' ⋄ suppress←0 ⋄ :EndIf
      r←''
      :Trap 0
          r←⎕SH cmd,' 2>/dev/null'
      :Else
          ('shell command failed: ',cmd)⎕SIGNAL 11/⍨~suppress
      :EndTrap
    ∇

    ∇ r←APLVersion
      :Select 3↑⊃'.'⎕WG'APLVersion'
      :CaseList 'Lin' 'AIX' 'Sol'
          r←'*nix'
      :Case 'Win'
          r←'Win'
      :Case 'Mac'
          r←'Mac'
      :Else
          ... ⍝ unknown version
      :EndSelect
    ∇
    :EndSection ⍝ Files

:EndClass