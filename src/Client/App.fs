module App

open Lit
open Fable.Core.JsInterop

importSideEffects "./index.css"

[<LitElement("my-app")>]
let MyApp() =
  let _ = LitElement.init (fun cfg -> cfg.useShadowDom <- false)
  html $"<p>Hello, World!</p>"
