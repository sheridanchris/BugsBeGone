module DataAccess

open System
open System.Data
open Donald
open System.Threading.Tasks

[<AutoOpen>]
module Utils =
  let sqlTypeOrNull (constructor: 'a -> SqlType) (value: 'a option) =
    match value with
    | Some value -> constructor value
    | None -> SqlType.Null

type User = {
  Id: Guid
  Username: string
  EmailAddress: string
  GravatarEmailAddress: string
  AccountVerified: bool
  PasswordHash: string
  Biography: string option
}

type Priority =
  | NotAssigned = 0
  | Low = 1
  | Medium = 2
  | High = 3
  | Urgent = 4

type Issue = {
  Id: Guid
  Title: string
  Description: string
  AuthorId: Guid
  AssignedUserId: Guid option
  Priority: Priority
  CreatedAt: DateTime
  UpdatedAt: DateTime
  IsClosed: bool
}

[<RequireQualifiedAccess>]
module User =
  let read (reader: IDataReader) = {
    Id = reader.ReadGuid "id"
    Username = reader.ReadString "username"
    EmailAddress = reader.ReadString "email_address"
    GravatarEmailAddress = reader.ReadString "gravatar_email_address"
    AccountVerified = reader.ReadBoolean "account_verified"
    PasswordHash = reader.ReadString "password_hash"
    Biography = reader.ReadStringOption "biography"
  }

[<RequireQualifiedAccess>]
module Issue =
  let read (reader: IDataReader) = {
    Id = reader.ReadGuid "id"
    Title = reader.ReadString "title"
    Description = reader.ReadString "description"
    AuthorId = reader.ReadGuid "author_id"
    AssignedUserId = reader.ReadGuidOption "assigned_user_id"
    Priority = enum (reader.ReadInt32 "priority")
    CreatedAt = reader.ReadDateTime "created_at"
    UpdatedAt = reader.ReadDateTime "updated_at"
    IsClosed = reader.ReadBoolean "is_closed"
  }

type InsertUser = User -> Task<Result<unit, DbError>>
type TryFindUserById = Guid -> Task<Result<User option, DbError>>

let insertUser (connection: IDbConnection) : InsertUser =
  fun user ->
    let insertCommand =
      "INSERT INTO users (id, username, email_address, gravatar_email_address, account_verified, password_hash, biography)
       VALUES (@id, @username, @email_address, @gravatar_email_address, @account_verified, @password_hash, @biography)"

    let parameters = [
      "id", SqlType.Guid user.Id
      "username", SqlType.String user.Username
      "email_address", SqlType.String user.EmailAddress
      "gravatar_email_address", SqlType.String user.GravatarEmailAddress
      "account_verified", SqlType.Boolean user.AccountVerified
      "password_hash", SqlType.String user.PasswordHash
      "biography", sqlTypeOrNull SqlType.String user.Biography
    ]

    connection
    |> Db.newCommand insertCommand
    |> Db.setParams parameters
    |> Db.Async.exec

let tryFindUserById (connection: IDbConnection) : TryFindUserById =
  fun userId ->
    connection
    |> Db.newCommand "SELECT * FROM users WHERE id = @id"
    |> Db.setParams [ "id", SqlType.Guid userId ]
    |> Db.Async.querySingle User.read

type Ordering =
  | NoOrdering
  | Title
  | HighestPriority
  | Latest
  | RecentlyUpdated

type FindIssueQuery = {
  Ordering: Ordering
  Page: int
  PageSize: int
}

type InsertIssue = Issue -> Task<Result<unit, DbError>>
type FindIssueById = Guid -> Task<Result<Issue option, DbError>>
type FindIssues = FindIssueQuery -> Task<Result<Issue list, DbError>>
type SearchIssuesByTitle = string -> Task<Result<Issue list, DbError>>

let insertIssue (connection: IDbConnection) : InsertIssue =
  fun issue ->
    let insertCommand =
      "INSERT INTO issues (id, title, description, author_id, assigned_user_id, priority, created_at, updated_at, is_closed)
       VALUES (@id, @title, @description, @author_id, @assigned_user_id, @priority, @created_at, @updated_at, @is_closed)"

    let parameters = [
      "id", SqlType.Guid issue.Id
      "title", SqlType.String issue.Title
      "description", SqlType.String issue.Description
      "author_id", SqlType.Guid issue.AuthorId
      "assigned_user_id", sqlTypeOrNull SqlType.Guid issue.AssignedUserId
      "priority", SqlType.Int32(int issue.Priority)
      "created_at", SqlType.DateTime issue.CreatedAt
      "updated_at", SqlType.DateTime issue.UpdatedAt
      "is_closed", SqlType.Boolean issue.IsClosed
    ]

    connection
    |> Db.newCommand insertCommand
    |> Db.setParams parameters
    |> Db.Async.exec

let findIssueById (connection: IDbConnection) : FindIssueById =
  fun issueId ->
    connection
    |> Db.newCommand "SELECT * FROM issues WHERE id = @id"
    |> Db.setParams [ "id", SqlType.Guid issueId ]
    |> Db.Async.querySingle Issue.read

let findIssues (connection: IDbConnection) : FindIssues =
  fun findIssueQuery ->
    let ordering =
      match findIssueQuery.Ordering with
      | Title -> "title"
      | HighestPriority -> "priority"
      | RecentlyUpdated -> "updated_at"
      | NoOrdering
      | Latest -> "created_at"

    let parameters = [
      "ordering", SqlType.String ordering
      "offset", SqlType.Int(findIssueQuery.Page * findIssueQuery.PageSize)
      "limit", SqlType.Int findIssueQuery.PageSize
    ]

    connection
    |> Db.newCommand "SELECT * FROM issues ORDER BY @ordering DESC OFFEST @offset LIMIT @limit"
    |> Db.setParams parameters
    |> Db.Async.query Issue.read

let searchIssuesByTitle (connection: IDbConnection) : SearchIssuesByTitle =
  fun title ->
    connection
    |> Db.newCommand "SELECT * FROM issues WHERE to_tsvector(title) @@ to_tsquery(@title)"
    |> Db.setParams [ "title", SqlType.String title ]
    |> Db.Async.query Issue.read
