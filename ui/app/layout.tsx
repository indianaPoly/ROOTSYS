import type { Metadata } from "next";
import { Manrope, IBM_Plex_Mono } from "next/font/google";
import Link from "next/link";
import type { ReactNode } from "react";

import { SmoothScroll } from "@/components/smooth-scroll";

import "./globals.css";

const headingFont = Manrope({
  subsets: ["latin"],
  variable: "--font-heading",
  weight: ["500", "600", "700", "800"]
});

const monoFont = IBM_Plex_Mono({
  subsets: ["latin"],
  variable: "--font-mono",
  weight: ["400", "500"]
});

export const metadata: Metadata = {
  title: "ROOTSYS",
  description: "Landing page and runtime console for ROOTSYS validation artifacts"
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body className={`${headingFont.variable} ${monoFont.variable}`}>
        <SmoothScroll />
        <a className="skipLink" href="#content">
          Skip to content
        </a>
        <header className="topNav" aria-label="Primary navigation">
          <div className="topNavInner">
            <Link className="brand" href="/">
              ROOTSYS
            </Link>
            <nav className="navLinks" aria-label="Site">
              <Link href="/">Overview</Link>
              <Link href="/analysis">Analysis</Link>
              <Link href="/analysis/ops">Ops Dashboard</Link>
              <Link href="/console">Runtime Console</Link>
            </nav>
          </div>
        </header>
        {children}
      </body>
    </html>
  );
}
