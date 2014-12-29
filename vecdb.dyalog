:Class vecdb
⍝ Vector database v0.1.2

    (⎕IO ⎕ML)←1 1

    :Section Constants
    :Field Public Shared TypeNames←'I1' 'I2' 'I4',,¨'FB'
    :Field Public Shared TypeNums←83 163 323 645 11
    :Field Public Shared Version←'0.1.2'
    :EndSection ⍝ Constants

    :Section Instance Fields
    :Field Public Name←''
    :Field Public Folder←''             ⍝ Where is it
    :Field Public Data←0⍴⎕ns ''         ⍝ One element per column: .v (value) .n (name) .t (typename)
    :Field Public Count←0               ⍝ Number of records
    :Field Public BlockSize←100000      ⍝ Small while we test (must be multiple of 8)
    :Field Public NumBlocks←1           ⍝ We start with one block
    :Field Public Size←0
    :Field Public Shards←0⍴⊂0⍴⎕NS ''    ⍝ Shards (not supported in v0.0)
    :Field Public noFiles←0             ⍝ in-memory database?
    :Field Public Open←0                ⍝ Not yet open

    :field _Columns←⍬
    :field _Types←⍬

    state←'Name' 'BlockSize'
    :EndSection ⍝ Instance Fields

    :section Properties
    :property Columns
    :access public
        ∇ r←get
          r←_Columns
        ∇
    :endproperty

    :property Types
    :access public
        ∇ r←get
          r←_Types
        ∇
    :endproperty
    :endsection

    ∇ make1(folder);tn;file;props;shards
    ⍝ Open an existing database
     
      :Implements constructor
      :Access Public
     
      folder←folder,((¯1↑folder)∊'/\')↓'/'
     
      :Trap 0
          tn←(file←folder,'meta.vecdb')⎕FSTIE 0
      :Else
          ('Unable to open ',file)⎕SIGNAL 11
      :EndTrap
      (Count NumBlocks)←⎕FREAD tn 3
      props←⎕FREAD tn 4 ⍝ database properties
      shards←⎕FREAD tn,5
      ⎕FUNTIE tn
      ⍎'(',(⍕1⊃props),')←2⊃props'
     
      Shards←{
          (⎕NS¨(≢⍵)⍴⊂''){
              ⍺{⍺}⍺⍎(⍕,¨⊃⍵),'←2⊃⍵'
          }¨⍵
      }¨shards
      Data←⊃Shards
      _Columns←Data.n
      _Types←Data.t
     
      (Folder Open Size)←folder 1(NumBlocks×BlockSize)
      MakeMaps
    ∇

    ∇ MakeMaps;s;i;types;d;t
    ⍝ [Re]make all maps
      types←TypeNums[TypeNames⍳Types]
     
      :For s :In Shards       ⍝ But we only support 1
          :For d t :InEach s types
              d.v←(t,¯1)⎕MAP(Folder,d.f)'W'
          :EndFor
      :EndFor
    ∇

    ∇ make4(name folder columns types)
      :Implements constructor
      :Access Public
      make6(name folder columns types'' '') ⍝ No data or option
    ∇

    ∇ make5(name folder columns types options)
      :Implements constructor
      :Access Public
      make6(name folder columns types options'') ⍝ No data or option
    ∇

    ∇ make6(name folder columns types options data);i;s;offset;file;tn;type;length
      :Implements constructor
      :Access Public
    ⍝ Create a new database
    ⍝ If folder is empty, do not create files to back it - just keep data in memory
     
      folder,←((¯1↑folder)∊'/\')↓'/' ⍝ make sure we have trailing separator
      :If Exists ¯1↓folder ⍝ Folder already exists
          file←folder,'meta.vecdb'
          ('"',file,'" already exists')⎕SIGNAL(Exists file)/11
      :Else ⍝ Folder does not exist
          :Trap 0 ⋄ MkDir ¯1↓folder
          :Else ⋄ ⎕DMX.Message ⎕SIGNAL ⎕DMX.EN
          :EndTrap
      :EndIf
     
    ⍝ Validate creation parameters
      'Database must have at least one column'⎕SIGNAL(1>≢columns)⍴11
      'Column types and names do not have same length'⎕SIGNAL((≢columns)≠≢types)⍴11
      'Invalid column types - see vecdb.TypeNames'⎕SIGNAL(∧/types∊TypeNames)↓11
     
      :If 0=≢data ⋄ data←(≢columns)⍴⊂⍬ ⋄ :EndIf ⍝ Default data is all zeros
      ProcessOptions options ⍝ Sets global fields
      'Block size must be a multiple of 8'⎕SIGNAL(0≠8|BlockSize)/11
     
      'Data lengths not all the same'⎕SIGNAL(1≠≢length←∪≢¨data)/11
      Size←BlockSize×NumBlocks←1⌈⌈length÷BlockSize ⍝ At least one block
     
      (Name Count)←name(⊃length) ⍝ Update real props
      Data←types{n←⍵ ⋄ t←⍺ ⋄ ⎕NS,¨'tn'}¨columns
      _Columns←Data.n
      _Types←Data.t
     
      ⍝ Harwired to only write one Shard
      :For s :In ⍳≢Shards←,⊂Data
          ExtendShard folder s Size data'create'
      :EndFor
     
      file←folder,'meta.vecdb'
      tn←file ⎕FCREATE 0
      ('vecdb ',Version)⎕FAPPEND tn ⍝ 1
      'Count and NumBlocks in component 3, other metadata in 4 and 5'⎕FAPPEND tn ⍝ 2
      (Count NumBlocks)⎕FAPPEND tn ⍝ 3
      (state(⍎¨state))⎕FAPPEND tn   ⍝ 4
      Shards.('ftn'(f t n))⎕FAPPEND tn ⍝ 5
     
      ⎕FUNTIE tn
     
      make1,⊂folder ⍝ now open it properly
    ∇

    ∇ ExtendShard(folder s rcds data mode);i;type;file;tn
    ⍝ Extend a
      :For i :In ⍳⍴Data
          type←(TypeNames⍳Types[i])⊃TypeNums
          Data[i].f←(⍕i),'_',(⍕s),'.vector'
          file←folder,Data[i].f
          :Select mode
          :Case 'create' ⋄ tn←file ⎕NCREATE 0
          :Case 'extend' ⋄ tn←file ⎕NTIE 0
          :EndSelect
          (rcds↑i⊃data)⎕NAPPEND tn,type
          ⎕NUNTIE tn
      :EndFor
    ∇

    ∇ r←Close
      :Access Public
      :If ~0∊⍴Shards
          Shards.⎕EX'v' ⍝ remove all maps
      :EndIf
      r←Open←0       ⍝ record the fact
    ∇

    ∇ unmake
      :Implements Destructor
      {}Close
    ∇

    ∇ ProcessOptions options;name
    ⍝ Extract optional fields from options
     
      :If 9=⎕NC'options'
          :For name :In options.⎕NL-2
              :If (⊂name)∊'BlockSize' 'NumBlocks'
                  ⍎name,'←options.',name
              :Else
                  ('Invalid option name: ',name)⎕SIGNAL 11
              :EndIf
          :EndFor
      :EndIf
     
    ∇

    ∇ r←Query(where cols);col;value;ix;j
      :Access Public
     
      :If (2=≢where)∧2=|≡where ⍝ single constraint
          where←,⊂where
      :EndIf
     
      ix←⎕NULL
      :For (col value) :In where ⍝ AND them all together
          j←Columns⍳⊂col
          ('Invalid column name(s): ',⍕col)⎕SIGNAL((⊂col)∊Columns)↓11
          :If ⎕NULL≡ix ⋄ ix←{⍵/⍳⍴⍵}(Count↑Data[j].v)∊value
          :Else ⋄ ix/⍨←Data[j].v[ix]∊value
          :EndIf
          :If 0=⍴ix ⋄ :Leave ⋄ :EndIf
      :EndFor
     
      :If 0=⍴cols ⋄ r←ix ⍝ No columns; Just return index
      :Else
          r←Read ix cols
      :EndIf
    ∇

    ∇ r←Read(indices cols)
      ⍝ Read specified insices of named columns
      :Access Public
     
      :If 1=≡cols ⋄ cols←,⊂cols ⋄ :EndIf ⍝ Single simple comlumn name
      ⎕SIGNAL/ValidateColumns cols
      r←(Data[Columns⍳cols]).{v[⍵]}⊂indices
    ∇

    ∇ r←ValidateColumns cols;bad
     ⍝ Return result suitable for ⎕SIGNAL/
     
      r←''⍬
      :If ~0∊⍴bad←cols~Columns
          r←('Unknown Column Names:',,⍕bad)11
      :EndIf
    ∇

    ∇ r←Append(cols data);length;canupdate;shards;s;alldata;growth;tn
      :Access Public
     
      'Data lengths not all the same'⎕SIGNAL(1≠≢length←∪≢¨data)/11
      'Col and Data counts not the same'⎕SIGNAL((≢cols)≠≢data)/11
      ⎕SIGNAL/ValidateColumns cols
     
      :If 0≠canupdate←length⌊Size-Count
          (⊂Count+⍳canupdate)(Data[Columns⍳cols]).{
              v[⍺]←⍵
          }canupdate↑¨data
      :EndIf
     
      :If length>canupdate    ⍝ We need to extend the file
          alldata←(≢Columns)⍴⊂⍬ ⋄ alldata[Columns⍳cols]←canupdate↓¨data
          growth←BlockSize×(length-canupdate)(⌈÷)BlockSize ⍝ How many records to add
          shards←≢Shards
          Shards.⎕EX'v'
          :For s :In shards
              Data←s⊃Shards
              ExtendShard Folder s growth alldata'extend'
          :EndFor
     
          MakeMaps            ⍝ remake all the maps
          Size+←growth
          NumBlocks←Size÷BlockSize
      :EndIf
      Count+←length
      tn←(Folder,'meta.vecdb')⎕FSTIE 0
      (Count NumBlocks)⎕FREPLACE tn 3
      ⎕FUNTIE tn
      r←0
    ∇

    ∇ {r}←Update(indices cols data)
      :Access Public
     
      :If 1=≡cols ⋄ (cols data)←,∘⊂¨cols data ⋄ :EndIf ⍝ Simple col name
      ⎕SIGNAL/ValidateColumns cols
      (⊂indices)(Data[Columns⍳cols]).{
          v[⍺]←⍵
      }data
      r←0
    ∇

    ∇ r←Erase;file;s;i;f;shards
      :Access Public
      ⍝ /// needs error trapping
     
      'vecdb is not open'⎕SIGNAL Open↓11
     
      :If Exists file←Folder,'meta.vecdb'
          shards←≢Shards
          {}Close
          :For Data :In Shards
              :For f :In Data.f
                  {⍵ ⎕NERASE ⍵ ⎕NTIE 0}Folder,f
              :EndFor
          :EndFor
          file ⎕NERASE file ⎕NTIE 0
          ⍝ RmDir Folder ⍝ /// This fails, RmDir needs to be fixed
          r←0
      :Else
          ('Not a vector db: ',Folder)⎕SIGNAL 2
      :EndIf
    ∇

    :Section Files
⍝ /// These functions should be really be :Included from Files

    ∇ ok←Exists path;FindFirstFileX;FindNextFileX;FindClose;FileTimeToLocalFileTime;FileTimeToSystemTime;GetLastError
      _FindDefine
      ok←0≢⊃_FindFirstFile path
    ∇

    ∇ _FindDefine;WIN32_FIND_DATA
      WIN32_FIND_DATA←'{I4 {I4 I4} {I4 I4} {I4 I4} {U4 U4} {I4 I4} T[260] T[14]}'
      'FindFirstFileX'⎕NA'P kernel32.C32|FindFirstFile* <0T >',WIN32_FIND_DATA
      'FindNextFileX'⎕NA'U4 kernel32.C32|FindNextFile* P >',WIN32_FIND_DATA
      ⎕NA'kernel32.C32|FindClose P'
      ⎕NA'I4 kernel32.C32|FileTimeToLocalFileTime <{I4 I4} >{I4 I4}'
      ⎕NA'I4 kernel32.C32|FileTimeToSystemTime <{I4 I4} >{I2 I2 I2 I2 I2 I2 I2 I2}'
      ⎕NA'I4 kernel32.C32|GetLastError'
    ∇

    ∇ rslt←_FindFirstFile name;⎕IO
      rslt←FindFirstFileX name(⎕IO←0)
      :If 1∊(¯1+2*32 64)=0⊃rslt       ⍝ INVALID_HANDLE_VALUE 32 or 64
          rslt←0 GetLastError
      :Else
          (1 6⊃rslt)_FindTrim←0        ⍝ shorten the file name at the null delimiter
          (1 7⊃rslt)_FindTrim←0        ⍝ and for the alternate name
      :EndIf
    ∇

    ∇ MkDir path;CreateDirectory;GetLastError;err
      ⍝ Create a folder using Win32 API
     
      ⎕NA'I kernel32.C32|CreateDirectory* <0T I4' ⍝ Try for best function
      →(0≠CreateDirectory path 0)⍴0 ⍝ 0 means "default security attributes"
      ⎕NA'I4 kernel32.C32|GetLastError'
      err ⎕SIGNAL⍨'CreateDirectory error:',⍕err←GetLastError
    ∇

    ∇ RmDir path;RemoveDirectory;GetLastError
     ⍝ Remove folder using Win32 API
     
      ⎕NA'I kernel32.C32|RemoveDirectory* <0T'
      →(0≠RemoveDirectory,⊂path)⍴0
      ⎕NA'I4 kernel32.C32|GetLastError'
      11 ⎕SIGNAL⍨'RemoveDirectory error:',⍕GetLastError
    ∇
    :EndSection ⍝ Files

:EndClass