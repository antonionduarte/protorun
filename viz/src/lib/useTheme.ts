import { useEffect, useState } from "react";

export type ThemeMode = "light" | "dark" | "system";

const KEY = "protoviz-theme";

/** Dark/light via the default shadcn mechanism: a `.dark` class on <html>.
 * Defaults to "system" and follows the OS preference live. */
export function useTheme(): [ThemeMode, (m: ThemeMode) => void] {
  const [mode, setMode] = useState<ThemeMode>(
    () => (localStorage.getItem(KEY) as ThemeMode) || "system"
  );

  useEffect(() => {
    const root = document.documentElement;
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const apply = () => {
      const dark = mode === "dark" || (mode === "system" && mq.matches);
      root.classList.toggle("dark", dark);
    };
    apply();
    localStorage.setItem(KEY, mode);
    if (mode === "system") {
      mq.addEventListener("change", apply);
      return () => mq.removeEventListener("change", apply);
    }
  }, [mode]);

  return [mode, setMode];
}
