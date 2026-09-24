"use client";

import { useEffect, useRef } from "react";
import Editor, { type Monaco, type OnMount } from "@monaco-editor/react";
import type { editor, IDisposable, Position } from "monaco-editor";
import type * as monacoEditor from "monaco-editor";
import { usePrefersDark } from "@/lib/usePrefersDark";

const CONNECTION_REF_LINE = /connectionRef:\s*(['"])?[\w-]*$/;

export default function YamlResourceEditor({
  value,
  onChange,
  connectionNames,
}: {
  value: string;
  onChange: (next: string) => void;
  connectionNames?: string[];
}) {
  const prefersDark = usePrefersDark();
  const completionRef = useRef<IDisposable | null>(null);

  useEffect(() => () => completionRef.current?.dispose(), []);

  const handleMount: OnMount = (_editor: editor.IStandaloneCodeEditor, monaco: Monaco) => {
    completionRef.current?.dispose();
    completionRef.current = monaco.languages.registerCompletionItemProvider("yaml", {
      triggerCharacters: [" ", ":"],
      provideCompletionItems(model: monacoEditor.editor.ITextModel, position: Position) {
        const names = connectionNames ?? [];
        if (names.length === 0) return { suggestions: [] };

        const lineUpToCursor = model.getLineContent(position.lineNumber).slice(0, position.column - 1);
        if (!CONNECTION_REF_LINE.test(lineUpToCursor)) return { suggestions: [] };

        const word = model.getWordUntilPosition(position);
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn,
        };
        return {
          suggestions: names.map((name) => ({
            label: name,
            kind: monaco.languages.CompletionItemKind.Value,
            insertText: name,
            range,
          })),
        };
      },
    });
  };

  return (
    <div className="overflow-hidden rounded-xl border border-border">
      <Editor
        height="24rem"
        language="yaml"
        value={value}
        onChange={(next) => onChange(next ?? "")}
        theme={prefersDark ? "vs-dark" : "vs"}
        onMount={handleMount}
        options={{
          minimap: { enabled: false },
          scrollBeyondLastLine: false,
          fontSize: 12,
          renderLineHighlight: "none",
          overviewRulerLanes: 0,
          padding: { top: 12, bottom: 12 },
        }}
      />
    </div>
  );
}
