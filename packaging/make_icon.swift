// 앱 아이콘(1024px PNG) 생성: 둥근 보라색 사각형 + 말풍선 두 개
import AppKit

let size: CGFloat = 1024
let out = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : "icon_1024.png"
let image = NSImage(size: NSSize(width: size, height: size))
image.lockFocus()
let rect = NSRect(x: 0, y: 0, width: size, height: size).insetBy(dx: 100, dy: 100)
let bg = NSBezierPath(roundedRect: rect, xRadius: 185, yRadius: 185)
NSGradient(starting: NSColor(calibratedRed: 0.42, green: 0.40, blue: 0.98, alpha: 1),
           ending: NSColor(calibratedRed: 0.27, green: 0.22, blue: 0.82, alpha: 1))!.draw(in: bg, angle: -90)

func bubble(_ r: NSRect, tailLeft: Bool, color: NSColor) {
    let p = NSBezierPath(roundedRect: r, xRadius: 70, yRadius: 70)
    let t = NSBezierPath()
    if tailLeft {
        t.move(to: NSPoint(x: r.minX + 70, y: r.minY + 10))
        t.line(to: NSPoint(x: r.minX + 20, y: r.minY - 70))
        t.line(to: NSPoint(x: r.minX + 150, y: r.minY + 10))
    } else {
        t.move(to: NSPoint(x: r.maxX - 70, y: r.minY + 10))
        t.line(to: NSPoint(x: r.maxX - 20, y: r.minY - 70))
        t.line(to: NSPoint(x: r.maxX - 150, y: r.minY + 10))
    }
    t.close()
    color.setFill(); p.fill(); t.fill()
}
bubble(NSRect(x: 200, y: 470, width: 470, height: 290), tailLeft: true, color: .white)
bubble(NSRect(x: 360, y: 230, width: 460, height: 260), tailLeft: false, color: NSColor(calibratedRed: 1.0, green: 0.82, blue: 0.30, alpha: 1))
for i in 0..<3 {
    let d = NSBezierPath(ovalIn: NSRect(x: 300 + CGFloat(i) * 95, y: 585, width: 60, height: 60))
    NSColor(calibratedRed: 0.33, green: 0.30, blue: 0.90, alpha: 1).setFill(); d.fill()
}
image.unlockFocus()
let rep = NSBitmapImageRep(data: image.tiffRepresentation!)!
try! rep.representation(using: .png, properties: [:])!.write(to: URL(fileURLWithPath: out))
