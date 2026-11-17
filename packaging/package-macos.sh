#!/usr/bin/env bash
# Agent Mail 맥 앱(.app)과 설치용 디스크 이미지(.dmg)를 만든다.
#
#   packaging/package-macos.sh                 # 서명까지
#   NOTARY_PROFILE=이름 packaging/package-macos.sh   # 서명 + Apple 공증 + 스테이플
#
# 공증 프로필은 한 번만 직접 만들어 두면 된다:
#   xcrun notarytool store-credentials 이름 --apple-id <애플ID> --team-id HBNVC7KCMH
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
VERSION="$(grep -m1 '^version' Cargo.toml | cut -d'"' -f2)"
APP_NAME="Agent Mail"
BUNDLE_ID="com.muzuksazo.agentmail"
SIGN_ID="${SIGN_ID:-Developer ID Application: Yeo Won Youn (HBNVC7KCMH)}"
OUT="$ROOT/target/dist-app"
APP="$OUT/$APP_NAME.app"
DMG="$OUT/AgentMail-$VERSION.dmg"

echo "== 1) 유니버설 실행 파일 빌드 ($VERSION)"
export MACOSX_DEPLOYMENT_TARGET=11.0
for t in aarch64-apple-darwin x86_64-apple-darwin; do
  cargo build --release --locked --target "$t" 2>&1 | grep -E "^error|Finished"
done

echo "== 2) .app 만들기"
rm -rf "$OUT"; mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"
lipo -create -output "$APP/Contents/MacOS/agent-mail-p2p" \
  target/aarch64-apple-darwin/release/agent-mail-p2p target/x86_64-apple-darwin/release/agent-mail-p2p

ICONSET="$OUT/AppIcon.iconset"; mkdir -p "$ICONSET"
swift packaging/make_icon.swift "$OUT/icon_src.png" >/dev/null
for s in 16 32 128 256 512; do
  sips -z $s $s "$OUT/icon_src.png" --out "$ICONSET/icon_${s}x${s}.png" >/dev/null
  sips -z $((s*2)) $((s*2)) "$OUT/icon_src.png" --out "$ICONSET/icon_${s}x${s}@2x.png" >/dev/null
done
iconutil -c icns "$ICONSET" -o "$APP/Contents/Resources/AppIcon.icns"
rm -rf "$ICONSET" "$OUT/icon_src.png"

cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key><string>$APP_NAME</string>
  <key>CFBundleDisplayName</key><string>$APP_NAME</string>
  <key>CFBundleIdentifier</key><string>$BUNDLE_ID</string>
  <key>CFBundleVersion</key><string>$VERSION</string>
  <key>CFBundleShortVersionString</key><string>$VERSION</string>
  <key>CFBundleExecutable</key><string>agent-mail-p2p</string>
  <key>CFBundleIconFile</key><string>AppIcon</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>LSMinimumSystemVersion</key><string>11.0</string>
  <!-- 창이 없는 앱: 더블클릭하면 브라우저에 단톡방이 열리고, 끄기는 화면의 [앱 끄기] 버튼으로 한다 -->
  <key>LSUIElement</key><true/>
  <key>NSHighResolutionCapable</key><true/>
</dict>
</plist>
PLIST

echo "== 3) 서명 ($SIGN_ID)"
codesign --force --options runtime --timestamp --sign "$SIGN_ID" "$APP"
codesign --verify --deep --strict --verbose=1 "$APP"

echo "== 4) .dmg 만들기"
STAGE="$OUT/dmg"; mkdir -p "$STAGE"
cp -R "$APP" "$STAGE/"
ln -s /Applications "$STAGE/Applications"
hdiutil create -quiet -volname "$APP_NAME" -srcfolder "$STAGE" -ov -format UDZO "$DMG"
rm -rf "$STAGE"
codesign --force --timestamp --sign "$SIGN_ID" "$DMG"

if [ -n "${NOTARY_PROFILE:-}" ]; then
  echo "== 5) Apple 공증"
  xcrun notarytool submit "$DMG" --keychain-profile "$NOTARY_PROFILE" --wait
  xcrun stapler staple "$DMG"
  spctl -a -vvv -t open --context context:primary-signature "$DMG" || true
else
  echo "== 5) 공증 건너뜀 (NOTARY_PROFILE 없음) — 받는 사람 맥에서 첫 실행 때 경고가 뜬다"
fi

shasum -a 256 "$DMG" | tee "$DMG.sha256"
echo "완료: $DMG"
